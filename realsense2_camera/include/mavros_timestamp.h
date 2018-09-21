//
// Created by mpantic on 19.09.18.
//

#ifndef REALSENSE2_CAMERA_MAVROS_TIMESTAMP_H
#define REALSENSE2_CAMERA_MAVROS_TIMESTAMP_H

#include "ros/ros.h"
#include <mavros_msgs/CamIMUStamp.h>
#include <mavros_msgs/CommandTriggerControl.h>
#include <sensor_msgs/Image.h>
#include <mutex>
#include <tuple>

namespace mavros_trigger
{

template<typename t_chanel_id, typename t_cache>
class MavrosTrigger{

  typedef boost::function<void(const t_chanel_id& channel, const ros::Time& new_stamp, const std::shared_ptr<t_cache>& cal)> caching_callback;

  typedef struct {
    uint32_t seq;
    ros::Time old_stamp;
    std::shared_ptr<t_cache> frame;
    double exposure;
  } cache_queue_type;

 public:
  MavrosTrigger(const std::set<t_chanel_id>& channel_set) :
  channel_set_(channel_set){

    ROS_WARN_STREAM("CHANNEL NUMBER" << channel_set_.size());
    for(t_chanel_id id : channel_set_){
      sequence_time_map_[id].clear();
    }
  }

  caching_callback callback;

  void setup(){
    // Set these for now...
    first_image_ = false;
    trigger_sequence_offset_ = 0;
    triggering_started_ = false;

    cam_imu_sub_ =
        nh_.subscribe("/mavros/cam_imu_sync/cam_imu_stamp", 100,
                     &MavrosTrigger::camImuStampCallback, this);
  }

  void start() {
    if(triggering_started_){
      //already started, ignore
      return;
    }
    start_lock.lock();
    ROS_WARN("TRIGG");
    // First subscribe to the messages so we don't miss any.'

    for(t_chanel_id id : channel_set_){
      sequence_time_map_[id].clear();
    }

    trigger_sequence_offset_ = 0;

    const std::string mavros_trigger_service = "/mavros/cmd/trigger_control";
    if (ros::service::exists(mavros_trigger_service, false)) {
      mavros_msgs::CommandTriggerControl req;
      req.request.trigger_enable = true;
      // This is NOT integration time, this is actually the sequence reset.
      req.request.integration_time = 1.0;

      ros::service::call(mavros_trigger_service, req);

      ROS_INFO("Called mavros trigger service! Success? %d Result? %d",
               req.response.success, req.response.result);
    } else {
      ROS_ERROR("Mavros service not available!");
    }

    first_image_ = true;
    triggering_started_ = true;
    start_lock.unlock();
  }

  bool channelValid(const t_chanel_id& channel){
    return channel_set_.count(channel) == 1;
  }

  void cacheFrame(const t_chanel_id& channel, const uint32_t seq, const ros::Time& original_stamp, double exposure,
      const std::shared_ptr<t_cache> frame){

    if(!channelValid(channel)){
      ROS_WARN_STREAM_ONCE(log_prefix_ << "cacheFrame called for unsynchronized channel.");
      return;
    }

    if(cache_queue_[channel].frame) {
      ROS_WARN_STREAM_THROTTLE(60, log_prefix_ << " Overwriting image queue! Make sure you're getting "
                                                  "Timestamps from mavros. This message will only print "
                                                  "once a minute.");
    }

    cache_queue_[channel].frame = frame;
    cache_queue_[channel].old_stamp = original_stamp;
    cache_queue_[channel].seq = seq;
    cache_queue_[channel].exposure = exposure;

    ROS_WARN_STREAM(log_prefix_ << "Cached frame w seq " << seq);
  }

  bool lookupSequenceStamp(const t_chanel_id& channel, const uint32_t seq, const ros::Time& old_stamp, double exposure, ros::Time* new_stamp,
                          bool from_image_queue = false ) {

    if(!channelValid(channel)){
      ROS_WARN_STREAM_ONCE(log_prefix_ << "lookupSequenceStamp called for unsynchronized channel.");
      return false;
    }

    if (sequence_time_map_[channel].empty()) {
      ROS_DEBUG_STREAM(log_prefix_ << " Sequence time map empty");
      return false;
    }

    // Handle case of first image where offset is not determined yet
    if (first_image_) {

      // Get the first from the sequence time map.
      auto it = sequence_time_map_[channel].begin();
      int32_t mavros_sequence = it->first;

      // Get offset between first frame sequence and mavros
      trigger_sequence_offset_ =
          mavros_sequence - static_cast<int32_t>(seq);

      ROS_INFO(
          "%s New header offset determined by channel %i: %d, from %d to %d, timestamp "
          "correction: %f seconds.",
          log_prefix_, channel,
          trigger_sequence_offset_, it->first, seq,
          it->second.toSec() - old_stamp.toSec());

      *new_stamp = it->second;
      *new_stamp = shiftTimestampToMidExposure(*new_stamp, exposure);

      first_image_ = false;
      sequence_time_map_[channel].erase(it);
      return true;
    }

    // Search correct sequence number and adjust
    auto it = sequence_time_map_[channel].find(seq + trigger_sequence_offset_);
    if (it == sequence_time_map_[channel].end()) {
      ROS_WARN_STREAM("Sequence not found " << seq+trigger_sequence_offset_ << " original seq# " << seq);

      if(seq<trigger_sequence_offset_ && from_image_queue){
        ROS_WARN_STREAM("REMOVED CACHE");
        return true;
      }

      return false;
    }

    ROS_INFO("[Mavros Triggering] Remapped seq %d to %d, %f to %f", seq,
              seq + trigger_sequence_offset_, old_stamp.toSec(),
              it->second.toSec());

    // Check for divergence (too large delay)
    const double kMinExpectedDelay = 0.0;
    const double kMaxExpectedDelay = 34.0 * 1e-3;

    double delay = old_stamp.toSec() - it->second.toSec();
    if (delay < kMinExpectedDelay || delay > kMaxExpectedDelay) {
      ROS_ERROR(
          "%s Delay out of bounds! Actual delay: %f s, min: "
          "%f s max: %f s. Resetting triggering on next image.",
          log_prefix_,
          delay, kMinExpectedDelay, kMaxExpectedDelay);
      triggering_started_ = false;
      first_image_ = true;
    }

    *new_stamp = it->second;
    *new_stamp = shiftTimestampToMidExposure(*new_stamp, exposure);
    sequence_time_map_[channel].erase(it);

    return true;
  }

  ros::Time shiftTimestampToMidExposure(const ros::Time& stamp,
                                        double exposure_us) {
    ros::Time new_stamp = stamp + ros::Duration(exposure_us * 1e-6 / 2.0);
    return new_stamp;
  }


  void camImuStampCallback(const mavros_msgs::CamIMUStamp& cam_imu_stamp) {
    start_lock.lock();
    if (!triggering_started_) {
      // Ignore stuff from before we *officially* start the triggering.
      // The triggering is techncially always running but...
      start_lock.unlock();
      return;
    }

    //ignore messages until they wrap back - we should receive a 0 eventually.
    if(first_image_ && cam_imu_stamp.frame_seq_id != 0){
      ROS_WARN("[Cam Imu Sync] Ignoring old messages");
      start_lock.unlock();
      return;

    }

    // insert stamps into sequence map for all channels
    // each channel can use each time stamp once.
    for(auto channel : channel_set_){
      sequence_time_map_[channel][cam_imu_stamp.frame_seq_id] = cam_imu_stamp.frame_stamp;
    }

    ROS_INFO(
        "[Cam Imu Sync] Received a new stamp for sequence number: %ld with "
        "stamp: %f",
        cam_imu_stamp.frame_seq_id, cam_imu_stamp.frame_stamp.toSec());


    // release potentially cached frames
    constexpr bool kFromImageQueue = true;

    for(auto channel : channel_set_) {

      if (!cache_queue_[channel].frame) {
        continue;
      }

      ros::Time new_stamp;

      // even if we do have to discard cached image due to a missing callback fct,
      // we want them to pass through the lookup logic, in order to not miss any sequence offsets etc.
      if (lookupSequenceStamp(channel, cache_queue_[channel].seq,
          cache_queue_[channel].old_stamp,
          cache_queue_[channel].exposure,
          &new_stamp, kFromImageQueue)) {
        if (callback) {
          callback(channel, new_stamp, cache_queue_[channel].frame);
        } else {
          ROS_WARN_STREAM_THROTTLE(10, log_prefix_ << " No callback set - discarding cached images.");
        }
        cache_queue_[channel].frame.reset();
      }
    }

    start_lock.unlock();
  }

 private:
  ros::NodeHandle nh_;
  bool first_image_;
  bool triggering_started_;
  int trigger_sequence_offset_ = 0;

  const std::set<t_chanel_id> channel_set_;
  ros::Subscriber cam_imu_sub_;
  std::map<t_chanel_id, std::map<uint32_t, ros::Time>> sequence_time_map_;
  std::mutex start_lock;

  std::map<t_chanel_id, std::string> logging_name_;
  std::map<t_chanel_id, cache_queue_type> cache_queue_;

  const std::string log_prefix_ = "[Mavros Triggering] ";
};


}

#endif //REALSENSE2_CAMERA_MAVROS_TIMESTAMP_H

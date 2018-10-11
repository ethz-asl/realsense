//
// Created by mpantic on 19.09.18.
//

#ifndef REALSENSE2_CAMERA_MAVROS_TIMESTAMP_H
#define REALSENSE2_CAMERA_MAVROS_TIMESTAMP_H

#include "ros/ros.h"
#include <mavros_msgs/CamIMUStamp.h>
#include <mavros_msgs/CommandTriggerControl.h>
#include <sensor_msgs/Image.h>
#include <boost/serialization/strong_typedef.hpp>
#include <geometry_msgs/PointStamped.h>
#include <mutex>
#include <queue>
#include <tuple>

// Note on multi threading:
//      To avoid any confusion and non-defined state, this class locks a mutex for every function call
//      that is not const. This is due to the fact that many callbacks can happen simultaneously, especially
//      on callback based drivers such as the realsense. If not locked properly, this can lead to weird states.


namespace mavros_trigger {
enum trigger_state {
  ts_synced = 1,
  ts_not_initalized,
  ts_wait_for_sync,   // MAVROS reset sent, but no image/timestamps correlated yet.
  ts_disabled
};

template<typename t_channel_id, typename t_cache>
class MavrosTrigger {

  // callback definition for processing cached frames (with type t_cache).
  typedef boost::function<void(const t_channel_id &channel,
                               const ros::Time &new_stamp,
                               const std::shared_ptr<t_cache> &cal)> caching_callback;

  // internal representation of a cached frame
  // t_cache is the external representation
  typedef struct {
    bool trigger_info;
    bool camera_info;

    uint32_t seq_camera;

    ros::Time stamp_camera;
    ros::Time stamp_trigger;
    std::shared_ptr<t_cache> frame;
    double exposure;
  } cache_queue_type;



 public:

  MavrosTrigger(const std::set<t_channel_id> &channel_set) :
      channel_set_(channel_set),
      state_(ts_not_initalized) {
    ROS_DEBUG_STREAM(log_prefix_ << " Initialized with " << channel_set_.size() << " channels.");
    for (t_channel_id id : channel_set_) {
      cache_queue_[id].clear();
    }
  }

  void setup(const caching_callback &callback) {
    std::lock_guard<std::mutex> lg(mutex_);

    trigger_sequence_offset_ = 0;
    delay_pub_ = nh_.advertise<geometry_msgs::PointStamped>("delay", 100);
    state_ = ts_not_initalized;
    callback_ = callback;
    cam_imu_sub_ =
        nh_.subscribe("/mavros/cam_imu_sync/cam_imu_stamp", 100,
                      &MavrosTrigger::camImuStampCallback, this);

    ROS_DEBUG_STREAM(log_prefix_ << " Callback set and subscribed to cam_imu_stamp");
  }

  void start() {
    std::lock_guard<std::mutex> lg(mutex_);

    if (state_ != ts_not_initalized) {
      //already started, ignore
      return;
    }

    ROS_INFO_STREAM(log_prefix_ << " Started triggering.");

    for (t_channel_id id : channel_set_) {
      cache_queue_[id].clear();
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
      state_ = ts_wait_for_sync;
    } else {
      ROS_ERROR("Mavros service not available!");
      state_ = ts_disabled;
    }
  }

  bool channelValid(const t_channel_id &channel) const {
    return channel_set_.count(channel) == 1;
  }


  bool syncOffset(const t_channel_id &channel, const uint32_t seq_camera, const ros::Time &stamp_camera) {
    // Get the first from the sequence time map.
    auto it = cache_queue_[channel].begin();
    if(!it->second.trigger_info){
      ROS_ERROR_STREAM(log_prefix_ << "First queue entry without trigger info - should never happen");
      return false;
    }

    // key is sequence of trigger
    uint32_t seq_trigger = it->first;

    // Get offset between first frame sequence and mavros
    trigger_sequence_offset_ =
        static_cast<int32_t>(seq_trigger) - static_cast<int32_t>(seq_camera);

    double delay = stamp_camera.toSec() - it->second.stamp_trigger.toSec();


    ROS_INFO(
        "%s New header offset determined by channel %i: %d, from %d to %d, timestamp "
        "correction: %f seconds.",
        log_prefix_, channel,
        trigger_sequence_offset_, it->first, seq_camera,
        delay);

    // Check for divergence (too large delay)
    const double kMinExpectedDelay = -17.0 * 1e-3;
    const double kMaxExpectedDelay = 17.0 * 1e-3;

    if (delay > kMaxExpectedDelay || delay < kMinExpectedDelay) {
      ROS_ERROR_STREAM(log_prefix_ << "Delay out of bounds: " << delay );
      state_ = ts_not_initalized;
      return false;
    }

    state_ = ts_synced;

    for (auto ch_id : channel_set_) {
      last_published_trigger_seq_[ch_id] = seq_trigger - 1;
    }

    return true;
  }

  void addCameraFrame(const t_channel_id &channel, const uint32_t camera_seq,
      const ros::Time &camera_stamp, const std::shared_ptr<t_cache>& frame, const double exposure){

    std::lock_guard<std::mutex> lg(mutex_);

    if (!channelValid(channel)) {
      ROS_WARN_STREAM_ONCE(log_prefix_ << "Unsynchronized channe - ignoring frame.");
      return;
    }

    if (state_ == ts_wait_for_sync) {
      if (!syncOffset(channel, camera_seq, camera_stamp)) {
        return;
      }
    }

    // Ignore frame, we are not synced yet by now
    if (state_ != ts_synced) {
      ROS_WARN_STREAM(log_prefix_ << " Trigger not synced yet - ignoring frame.");
      return;
    }

    // add frame to cache

    // get trigger sequence for this camera frame
    const uint32_t trigger_seq = cameraToTriggerSeq(camera_seq);

    // get or create cache item for this trigger_seq
    cache_queue_type& cache_entry = cache_queue_[channel][trigger_seq];

    if(!cache_entry.camera_info)
    {
      // complete information if needed
      cache_entry.camera_info = true;
      cache_entry.seq_camera = camera_seq;
      cache_entry.stamp_camera = camera_stamp;
      cache_entry.frame = frame;
      cache_entry.exposure = exposure;
    }

    releaseCacheEntries(channel);
  }



  ros::Time shiftTimestampToMidExposure(const ros::Time &stamp,
                                        const double exposure_us) const {

    ros::Time new_stamp = stamp + ros::Duration(exposure_us * 1e-6 / 2.0) + ros::Duration(9.25 * 1e-3);
    return new_stamp;
  }

  void camImuStampCallback(const mavros_msgs::CamIMUStamp &cam_imu_stamp) {
    mutex_.lock(); // we don't use a lock guard here because we want to release before function ends.

    if (state_ == ts_not_initalized) {
      // Ignore stuff from before we *officially* start the triggering.
      // The triggering is techncially always running but...
      mutex_.unlock();
      return;
    }

    //ignore messages until they wrap back - we should receive a 0 eventually.
    if (state_ == ts_wait_for_sync && cam_imu_stamp.frame_seq_id != 0) {
      ROS_WARN("[Cam Imu Sync] Ignoring old messages, clearing queues");
      for (auto channel : channel_set_) {
        cache_queue_[channel].clear();
      }
      mutex_.unlock();
      return;
    }

    // insert stamps into sequence map for all channels
    // each channel can use each time stamp once.
    for (auto channel : channel_set_) {
      processTriggerMessage(channel, cam_imu_stamp.frame_seq_id, cam_imu_stamp.frame_stamp);
    }

    ROS_INFO(
        "[Cam Imu Sync] Received a new stamp for sequence number: %ld (%ld) with "
        "stamp: %f",
        cam_imu_stamp.frame_seq_id,
        cam_imu_stamp.frame_seq_id - trigger_sequence_offset_,
        cam_imu_stamp.frame_stamp.toSec());

    // release waiting cache items
    for (auto channel : channel_set_) {
      releaseCacheEntries(channel);
    }

    mutex_.unlock(); // unlock here to prevent cascading locks. for now..

  }

  inline uint32_t triggerToCameraSeq(const uint32_t trigger_seq){
    return trigger_seq - trigger_sequence_offset_;
  }

  inline uint32_t cameraToTriggerSeq(const uint32_t camera_seq){
    return camera_seq + trigger_sequence_offset_;
  }

  void processTriggerMessage(const t_channel_id& channel, const uint32_t trigger_seq, const ros::Time& trigger_time){

    // get or create cache item for this trigger_seq
    cache_queue_type& cache_entry = cache_queue_[channel][trigger_seq];

    if(!cache_entry.trigger_info){

      // complete information if needed
      cache_queue_type& cache_entry = cache_queue_[channel][trigger_seq];
      cache_entry.trigger_info = true;
      cache_entry.stamp_trigger = trigger_time;
    }
  }

  void releaseCacheEntries(const t_channel_id& channel){
    bool entry_released = false;

    // release all entries that are complete.
    do {
      uint32_t seq_trigger = last_published_trigger_seq_[channel] + 1;

      // check if "next" trigger sequence exists and is ready to be released
      auto it_next_sequence = cache_queue_[channel].find(seq_trigger);


      // if item exists and has all necessary info, release it
      entry_released = (it_next_sequence != cache_queue_[channel].end()) &&
          (it_next_sequence->second.camera_info && it_next_sequence->second.trigger_info);

      if(entry_released) {
        auto entry = it_next_sequence->second;

        // adjust timestamp
        ros::Time midpoint_exposure = shiftTimestampToMidExposure(entry.stamp_trigger,
                                                                   entry.exposure);

        // call callback to publish
        callback_(channel, midpoint_exposure, entry.frame);

        // output delay message and log
        double delay = midpoint_exposure.toSec() - entry.stamp_camera.toSec();
        geometry_msgs::PointStamped msg;
        msg.header.stamp = midpoint_exposure;
        msg.point.x = delay;
        msg.point.y = entry.seq_camera;
        msg.point.z = seq_trigger;
        delay_pub_.publish(msg);

        ROS_INFO_STREAM(log_prefix_ << "|cache|= " << cache_queue_[channel].size() - 1 << " Frame w seq_c=" << entry.seq_camera << ", seq_t=" << seq_trigger << " released w t_diff=" << delay);

        // cleanup
        last_published_trigger_seq_[channel]++;
        cache_queue_[channel].erase(it_next_sequence);

      } else {
        ROS_INFO_STREAM(log_prefix_ << "|cache|= " << cache_queue_[channel].size() << ". ");
      }

    } while(entry_released);

  }



 private:
  ros::NodeHandle nh_;

  const std::set<t_channel_id> channel_set_;
  int trigger_sequence_offset_ = 0;

  ros::Subscriber cam_imu_sub_;
  ros::Publisher delay_pub_;
  //std::map<t_chanel_id, std::map<uint32_t, ros::Time>> sequence_time_map_;

  std::mutex mutex_;

  caching_callback callback_;
  trigger_state state_;

  std::map<t_channel_id, std::map<uint32_t, cache_queue_type>> cache_queue_;
  std::map<t_channel_id, int32_t> last_published_trigger_seq_;

  const std::string log_prefix_ = "[Mavros Triggering] ";
};

}

#endif //REALSENSE2_CAMERA_MAVROS_TIMESTAMP_H

//
// Created by mpantic on 19.09.18.
//

#ifndef REALSENSE2_CAMERA_MAVROS_TIMESTAMP_H
#define REALSENSE2_CAMERA_MAVROS_TIMESTAMP_H

#include "ros/ros.h"
#include <mavros_msgs/CamIMUStamp.h>
#include <mavros_msgs/CommandTriggerControl.h>
#include <sensor_msgs/Image.h>
#include <geometry_msgs/PointStamped.h>
#include <mutex>
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

template<typename t_chanel_id, typename t_cache>
class MavrosTrigger {

  // callback definition for processing cached frames (with type t_cache).
  typedef boost::function<void(const t_chanel_id &channel,
                               const ros::Time &new_stamp,
                               const std::shared_ptr<t_cache> &cal)> caching_callback;

  // internal representation of a cached frame
  // t_cache is the external representation
  typedef struct {
    uint32_t seq;
    ros::Time old_stamp;
    std::shared_ptr<t_cache> frame;
    double exposure;
  } cache_queue_type;

 public:

  MavrosTrigger(const std::set<t_chanel_id> &channel_set) :
      channel_set_(channel_set),
      state_(ts_not_initalized) {
    ROS_DEBUG_STREAM(log_prefix_ << " Initialized with " << channel_set_.size() << " channels.");
    for (t_chanel_id id : channel_set_) {
      sequence_time_map_[id].clear();
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

    for (t_chanel_id id : channel_set_) {
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
      state_ = ts_wait_for_sync;
    } else {
      ROS_ERROR("Mavros service not available!");
      state_ = ts_disabled;
    }
  }

  bool channelValid(const t_chanel_id &channel) const {
    return channel_set_.count(channel) == 1;
  }

  void cacheFrame(const t_chanel_id &channel, const uint32_t seq, const ros::Time &original_stamp, double exposure,
                  const std::shared_ptr<t_cache> frame) {
    mutex_.lock();
    if (state_ != ts_synced) {
      mutex_.unlock();
      return;
    }

    if (!channelValid(channel)) {
      mutex_.unlock();
      ROS_WARN_STREAM_ONCE(log_prefix_ << "cacheFrame called for invalid channel.");
      return;
    }

    if(cache_queue_[channel].frame){
      mutex_.unlock();
      ros::spinOnce();
      mutex_.lock();
    }

    if (cache_queue_[channel].frame) {

      ROS_WARN_STREAM_THROTTLE(10, log_prefix_ << " Overwriting image queue! Make sure you're getting "
                                                  "Timestamps from mavros. This message will only print "
                                                  "every 10 sec.");
    }

    // set chached frame
    cache_queue_[channel].frame = frame;
    cache_queue_[channel].old_stamp = original_stamp;
    cache_queue_[channel].seq = seq;
    cache_queue_[channel].exposure = exposure;
    ROS_DEBUG_STREAM(log_prefix_ << "Cached frame w seq " << seq);
    mutex_.unlock();
  }

  bool syncOffset(const t_chanel_id &channel, const uint32_t seq, const ros::Time &old_stamp) {
    // Get the first from the sequence time map.
    auto it = sequence_time_map_[channel].begin();
    int32_t mavros_sequence = it->first;

    // Get offset between first frame sequence and mavros
    trigger_sequence_offset_ =
        mavros_sequence - static_cast<int32_t>(seq);

    double delay = old_stamp.toSec() - it->second.toSec();


    ROS_INFO(
        "%s New header offset determined by channel %i: %d, from %d to %d, timestamp "
        "correction: %f seconds.",
        log_prefix_, channel,
        trigger_sequence_offset_, it->first, seq,
        delay);

    // Check for divergence (too large delay)
    const double kMinExpectedDelay = 0.0;
    const double kMaxExpectedDelay = 34.0 * 1e-3;

    if (delay > kMaxExpectedDelay) {
      ROS_ERROR(
          "%s Delay out of bounds! Actual delay: %f s, min: "
          "%f s max: %f s. Resetting triggering on next image.",
          log_prefix_,
          delay, kMinExpectedDelay, kMaxExpectedDelay);
      state_ = ts_not_initalized;
      return false;
    }

    if (delay < 0.0) {
      ROS_ERROR_STREAM(log_prefix_ << " Ignoring negative time offset");
      // this forces synchronization to always be "forward" - the camera
      // frame received after the trigger signal is assigned the triggering timestamp.
      // Note that this could, depending on camera/hardware be wrong.
      return false;
    }

    state_ = ts_synced;
    return true;

  }

  bool lookupSequenceStamp(const t_chanel_id &channel, const uint32_t seq,
                           const ros::Time &old_stamp, double exposure, ros::Time *new_stamp,
                           bool from_image_queue = false) {

    std::lock_guard<std::mutex> lg(mutex_);

    if (!channelValid(channel)) {
      ROS_WARN_STREAM_ONCE(log_prefix_ << "lookupSequenceStamp called for unsynchronized channel.");
      return false;
    }

    if (sequence_time_map_[channel].empty()) {
      ROS_DEBUG_STREAM(log_prefix_ << " Sequence time map empty");
      return false;
    }


    // Unsynced state and cached images => cannot determine offset.
    if (state_ == ts_wait_for_sync && from_image_queue) {
      ROS_ERROR_STREAM(log_prefix_ << " Received cached image without sync, potentially wrong timestamps published");
      return true; // return true to process the frame nevertheless. So it doesn't go to cache again
    }

    // Unsynced state and "fresh" image => try determining offset.
    bool newly_synchronized = false;
    if (state_ == ts_wait_for_sync && !from_image_queue) {
      if (!(newly_synchronized =syncOffset(channel, seq, old_stamp))) {
        return false;
      }
    }

    // Synchronized state - we also hit this if we synced successfully during the same function call
    if (state_ == ts_synced) {
      // Search correct sequence number and adjust
      auto it = sequence_time_map_[channel].find(seq + trigger_sequence_offset_);

      // if we haven't found the sequence
      if (it == sequence_time_map_[channel].end()) {
        ROS_WARN_STREAM(
            log_prefix_ << "Sequence not found " << seq + trigger_sequence_offset_ << " original seq# " << seq);

        /* Not sure if needed / meaningful:
        if (seq < trigger_sequence_offset_ && from_image_queue) {
          ROS_WARN_STREAM("REMOVED CACHE");
          state_ = ts_not_initalized; // reset
          return true;
        }*/

        return false;
      }

      *new_stamp = it->second;
      *new_stamp = shiftTimestampToMidExposure(*new_stamp, exposure);
      sequence_time_map_[channel].erase(it);
    }

    double delay = old_stamp.toSec() - new_stamp->toSec();

    // if we made it until here, it's a successful match.
    geometry_msgs::PointStamped msg;
    msg.header.stamp = *new_stamp;
    msg.point.x = delay;
    msg.point.y = newly_synchronized ? 1.0 : 0.0; // Mark new synchronization by a 1
    delay_pub_.publish(msg);

    ROS_INFO_STREAM(log_prefix_ << " Remapped sequence " << seq << " -> " << seq + trigger_sequence_offset_ <<
    " and time " <<  old_stamp.toSec() << " -> " << new_stamp->toSec() << " ~ " << delay);

    return true;
  }

  ros::Time shiftTimestampToMidExposure(const ros::Time &stamp,
                                        double exposure_us) const {

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
      ROS_WARN("[Cam Imu Sync] Ignoring old messages");
      mutex_.unlock();
      return;
    }

    // insert stamps into sequence map for all channels
    // each channel can use each time stamp once.
    for (auto channel : channel_set_) {
      sequence_time_map_[channel][cam_imu_stamp.frame_seq_id] = cam_imu_stamp.frame_stamp;
    }

    ROS_INFO(
        "[Cam Imu Sync] Received a new stamp for sequence number: %ld (%ld) with "
        "stamp: %f",
        cam_imu_stamp.frame_seq_id,
        cam_imu_stamp.frame_seq_id - trigger_sequence_offset_,
        cam_imu_stamp.frame_stamp.toSec());

    mutex_.unlock(); // unlock here to prevent cascading locks. for now..

    // release potentially cached frames
    constexpr bool kFromImageQueue = true;

    for (auto channel : channel_set_) {

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
        if (callback_) {
          ROS_DEBUG_STREAM(log_prefix_ << "Released image w seq " << cache_queue_[channel].seq);

          callback_(channel, new_stamp, cache_queue_[channel].frame);
        } else {
          ROS_WARN_STREAM_THROTTLE(10, log_prefix_ << " No callback set - discarding cached images.");
        }
        cache_queue_[channel].frame.reset();
      }
    }
  }

 private:
  ros::NodeHandle nh_;

  const std::set<t_chanel_id> channel_set_;
  int trigger_sequence_offset_ = 0;

  ros::Subscriber cam_imu_sub_;
  ros::Publisher delay_pub_;
  std::map<t_chanel_id, std::map<uint32_t, ros::Time>> sequence_time_map_;
  std::mutex mutex_;

  caching_callback callback_;
  trigger_state state_;

  std::map<t_chanel_id, std::string> logging_name_;
  std::map<t_chanel_id, cache_queue_type> cache_queue_;

  const std::string log_prefix_ = "[Mavros Triggering] ";
};

}

#endif //REALSENSE2_CAMERA_MAVROS_TIMESTAMP_H

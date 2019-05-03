/*
 Copyright (c) 2018, Michael Pantic, ASL, ETH Zurich, Switzerland
                     Helen Oleynikova, ASL, ETH Zurich, Switzerland
                     Zac Taylor, ASL, ETH Zurich, Switzerland

 You can contact the author at <michael.pantic@mavt.ethz.ch>
 All rights reserved.
 Redistribution and use in source and binary forms, with or without
 modification, are permitted provided that the following conditions are met:
 * Redistributions of source code must retain the above copyright
 notice, this list of conditions and the following disclaimer.
 * Redistributions in binary form must reproduce the above copyright
 notice, this list of conditions and the following disclaimer in the
 documentation and/or other materials provided with the distribution.
 * Neither the name of ETHZ-ASL nor the
 names of its contributors may be used to endorse or promote products
 derived from this software without specific prior written permission.
 THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 DISCLAIMED. IN NO EVENT SHALL ETHZ-ASL BE LIABLE FOR ANY
 DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

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

// Note on multi threading / locking:
//      To avoid any confusion and non-defined state, this class locks a mutex for every function call
//      that is not const. This is due to the fact that many callbacks can happen simultaneously, especially
//      on callback based drivers such as the realsense. If not locked properly, this can lead to weird states.
//      Note that all private functions assume that they are called in a locked context (no cascading locks).
namespace mavros_trigger {

enum class TriggerState {
  kSynced = 0,      // Sequence offset synced between cam/trigger
  kNotInitialized,
  kWaitForSync,   // MAVROS reset sent, but no image/timestamps correlated yet.
  kDisabled
};

template<typename t_channel_id, typename t_cache>
class MavrosTrigger {

  const std::string kDelayTopic = "delay";
  const std::string kCamImuSyncTopic = "cam_imu_stamp";
  const std::string kTriggerService = "trigger_control";
  const std::string kLogPrefix = "[Mavros Triggering] ";

  // callback definition for processing cached frames (with type t_cache).
  typedef boost::function<void(const t_channel_id &channel,
                               const ros::Time &new_stamp,
                               const std::shared_ptr<t_cache> &cal)> caching_callback;

  // internal representation of a cached frame
  // t_cache is the external representation
  typedef struct {
    bool trigger_info;  // true if trigger information of a frame is filled in
    bool camera_info;   // true if camera information of a frame is filled in
    uint32_t seq_camera;
    ros::Time stamp_camera;
    ros::Time stamp_trigger;
    std::shared_ptr<t_cache> frame;
    double exposure;    // [us]
  } cache_queue_type;

 public:
  MavrosTrigger(const std::set<t_channel_id> &channel_set, bool allow_shifted_trigger_seq = true) :
      allow_shifted_trigger_seq_(allow_shifted_trigger_seq),
      channel_set_(channel_set),
      state_(TriggerState::kNotInitialized) {
    ROS_DEBUG_STREAM(kLogPrefix << " Initialized with " << channel_set_.size() << " channels.");
    for (t_channel_id id : channel_set_) {
      cache_queue_[id].clear();
    }
  }

  void setup(const caching_callback &callback, const double framerate, const double static_time_shift = 0.0) {
    std::lock_guard<std::mutex> lg(mutex_);

    trigger_sequence_offset_ = 0;
    delay_pub_ = nh_.advertise<geometry_msgs::PointStamped>(kDelayTopic, 100);
    state_ = TriggerState::kNotInitialized;
    framerate_ = framerate; // approximate framerate to do outlier checking
    callback_ = callback;
    static_time_shift_ = static_time_shift; // constant shift for bus transfers, sync-signal weirdness etc.
    // Values for realsense infra channel:
    // +10*1e-3 for 640x480, -6*1e-3 for 1280x720

    cam_imu_sub_ =
        nh_.subscribe(kCamImuSyncTopic, 100,
                      &MavrosTrigger::camImuStampCallback, this);

    ROS_INFO_STREAM(kLogPrefix << " Initialized with framerate " << framerate <<
    " hz, static time offset "<< static_time_shift_<< " and subscribed to cam_imu_sub");
  }

  void start() {
    std::lock_guard<std::mutex> lg(mutex_);

    if (state_ != TriggerState::kNotInitialized) {
      //already started, nothing to do
      return;
    }

    ROS_INFO_STREAM(kLogPrefix << " Started triggering.");

    for (t_channel_id id : channel_set_) {
      cache_queue_[id].clear();
    }

    trigger_sequence_offset_ = 0;

    if (ros::service::exists(kTriggerService, false)) {
      mavros_msgs::CommandTriggerControl req;
      req.request.trigger_enable = true;
      // This is NOT integration time, this is actually the sequence reset.
      req.request.cycle_time = 1.0;

      ros::service::call(kTriggerService, req);

      ROS_DEBUG("Called mavros trigger service! Success? %d Result? %d",
                req.response.success, req.response.result);
      state_ = TriggerState::kWaitForSync;
    } else {
      ROS_ERROR("Mavros service not available!");
      state_ = TriggerState::kDisabled;
    }

  }

  void waitMavros() {
    ROS_INFO_STREAM("Waiting for mavros service....");
    if (ros::service::waitForService(kTriggerService, ros::Duration(10))) {
      ROS_INFO_STREAM("... mavros service ready.");
    } else {
      ROS_ERROR_STREAM("... mavros service wait timeout.");
      // service disable is handled in start() function
    }
  }

  /*
   * Adds a timestamp and sequence number of the camera without caching/publishing the image
   *  - Keeps sync alive if images are not processed (e.g. because not needed)
   */
  void addCameraSequence(const t_channel_id &channel, const uint32_t camera_seq, const ros::Time &camera_stamp) {
    std::shared_ptr<t_cache> empty = {};
    addCameraFrame(channel, camera_seq, camera_stamp, empty, 0.0);
  }

  /*
   * Adds a timestamp, sequence number and image frame to be published by the callback once it's timing is corrected.
   *  - Caches t_cache frame pointer until corresponding trigger time message from mavros is received
   */
  void addCameraFrame(const t_channel_id &channel, const uint32_t camera_seq,
                      const ros::Time &camera_stamp, const std::shared_ptr<t_cache> &frame, const double exposure) {

    // allow threading subsystem to check for new IMU messages right before we lock.
    ros::spinOnce();

    std::lock_guard<std::mutex> lg(mutex_);

    if (!channelValid(channel)) {
      ROS_WARN_STREAM_ONCE(kLogPrefix << "Unsynchronized channe - ignoring frame.");
      return;
    }

    if (state_ == TriggerState::kWaitForSync) {
      if (!syncOffset(channel, camera_seq, camera_stamp)) {
        return;
      }
    }

    // Ignore frame, we are not synced yet by now
    if (state_ != TriggerState::kSynced) {
      ROS_WARN_STREAM(kLogPrefix << " Trigger not synced yet - ignoring frame.");
      return;
    }

    // add frame to cache
    // get trigger sequence for this camera frame
    const uint32_t trigger_seq = cameraToTriggerSeq(camera_seq);
    ROS_DEBUG_STREAM(kLogPrefix << " Received camera frame for seq " << trigger_seq);

    // get or create cache item for this trigger_seq
    cache_queue_type &cache_entry = cache_queue_[channel][trigger_seq];

    if (!cache_entry.camera_info) {
      // complete information if needed
      cache_entry.camera_info = true;
      cache_entry.seq_camera = camera_seq;
      cache_entry.stamp_camera = camera_stamp;
      cache_entry.frame = frame;
      cache_entry.exposure = exposure;
    }

    releaseCacheEntries(channel);
  }

  /*
   * Processes IMU-Stamp from MAVROS
   */
  void camImuStampCallback(const mavros_msgs::CamIMUStamp &cam_imu_stamp) {
    std::lock_guard<std::mutex> lg(mutex_);

    if (state_ == TriggerState::kNotInitialized) {
      // Ignore stuff from before we *officially* start the triggering.
      // The triggering is techncially always running but...
      return;
    }

    //ignore messages until they wrap back - we should receive a 0 eventually.
    if (state_ == TriggerState::kWaitForSync && cam_imu_stamp.frame_seq_id != 0) {
      ROS_WARN("[Cam Imu Sync] Ignoring old messages, clearing queues");
      return;
    }

    // insert stamps into sequence map for all channels
    // each channel can use each time stamp once.
    for (auto channel : channel_set_) {
      processTriggerMessage(channel, cam_imu_stamp.frame_seq_id, cam_imu_stamp.frame_stamp);
    }

    ROS_DEBUG(
        "[Cam Imu Sync] Received a new stamp for sequence number: %i (%i) with "
        "stamp: %f",
        cam_imu_stamp.frame_seq_id,
        cam_imu_stamp.frame_seq_id - trigger_sequence_offset_,
        cam_imu_stamp.frame_stamp.toSec());

    // release waiting cache items
    for (auto channel : channel_set_) {
      releaseCacheEntries(channel);
    }
  }

  bool supportsChannel(const t_channel_id &channel) const {
    return channelValid(channel);
    // implementation of external supportsChannel may differ from
    // internal channelValid in future - therefore seperate interface.
  }

  void approximateTriggerTime(const ros::Time &camera_time, ros::Time *trigger_time) {
    *trigger_time = camera_time + ros::Duration(last_delay_);
  }

 private:
  bool channelValid(const t_channel_id &channel) const {
    return channel_set_.count(channel) == 1;
  }

  /*
   * Tries to determine offset between camera and trigger/imu and returns true if succeeded
   */
  bool syncOffset(const t_channel_id &channel, const uint32_t seq_camera, const ros::Time &stamp_camera) {
    if (cache_queue_[channel].empty()) {
      ROS_WARN_STREAM_ONCE(kLogPrefix << " syncOffset called with empty queue");
      return false;
    }

    // Get the first from the sequence time map.
    auto it = cache_queue_[channel].begin();
    if (!it->second.trigger_info) {
      ROS_ERROR_STREAM(kLogPrefix << "First queue entry without trigger info - should never happen");
      return false;
    }

    // key is sequence of trigger
    uint32_t seq_trigger = it->first;

    // Get offset between first frame sequence and mavros
    trigger_sequence_offset_ =
        static_cast<int32_t>(seq_trigger) - static_cast<int32_t>(seq_camera);
    double delay = stamp_camera.toSec() - it->second.stamp_trigger.toSec();

    // Check for divergence (too large delay)
    double delay_bounds = 1.0 / framerate_;

    if (std::abs(delay) > delay_bounds &&
        std::abs(delay - delay_bounds) <= delay_bounds
        && allow_shifted_trigger_seq_) {
      ROS_INFO_STREAM(kLogPrefix << "Performing init with shifted trigger_sequence_offset!");
      trigger_sequence_offset_ += 1;
    } else if (std::abs(delay) > delay_bounds) {
      ROS_ERROR_STREAM(kLogPrefix << "Delay out of bounds: " << delay << ", bounds are +/-" << delay_bounds << " s");
      state_ = TriggerState::kNotInitialized;
      return false;
    }

    // if we survived until here, node is synchronized
    state_ = TriggerState::kSynced;
    for (auto ch_id : channel_set_) {
      last_published_trigger_seq_[ch_id] = seq_trigger;
    }

    ROS_INFO(
        "%s New header offset determined by channel %ld: %d, from %d to %d, timestamp "
        "correction: %f seconds.",
        kLogPrefix.c_str(), distance(channel_set_.begin(), channel_set_.find(channel)),
        trigger_sequence_offset_, it->first, seq_camera,
        delay);
    return true;
  }

  ros::Time shiftTimestampToMidExposure(const ros::Time &stamp,
                                        const double exposure_us) const {
    ros::Time new_stamp = stamp + ros::Duration(exposure_us * 1e-6 / 2.0) + ros::Duration(static_time_shift_);
    return new_stamp;
  }

  inline uint32_t cameraToTriggerSeq(const uint32_t camera_seq) {
    return camera_seq + trigger_sequence_offset_;
  }

  void processTriggerMessage(const t_channel_id &channel, const uint32_t trigger_seq, const ros::Time &trigger_time) {
    // get or create cache item for this trigger_seq
    cache_queue_type &cache_entry = cache_queue_[channel][trigger_seq];

    if (!cache_entry.trigger_info) {
      // complete information if needed
      cache_queue_type &cache_entry = cache_queue_[channel][trigger_seq];
      cache_entry.trigger_info = true;
      cache_entry.stamp_trigger = trigger_time;
    }
  }

  /*
   * Checks all cache entries in cache_queue_ and publishes those that
   *  - have both camera info and trigger info filled out
   *  - have sequence_id = last_published_trigger_seq_ + 1 ( so we only publish in order)
   *  Runs as long as messages are available that satisfy this.
   */
  void releaseCacheEntries(const t_channel_id &channel) {
    bool entry_released = false;

    // release all entries that are complete.
    do {
      uint32_t seq_trigger = last_published_trigger_seq_[channel] + 1;

      // check if "next" trigger sequence exists and is ready to be released
      auto it_next_sequence = cache_queue_[channel].find(seq_trigger);

      // if item exists and has all necessary info, release it
      entry_released = (it_next_sequence != cache_queue_[channel].end()) &&
          (it_next_sequence->second.camera_info && it_next_sequence->second.trigger_info);

      if (entry_released) {
        auto entry = it_next_sequence->second;

        // adjust timestamp
        ros::Time midpoint_exposure = shiftTimestampToMidExposure(entry.stamp_trigger,
                                                                  entry.exposure);

        // callback to publish if there is a cached frame
        //  - sometimes we add camera sequences without cached frames, e.g. if there are no subscribers
        //  - e.g. by calling addCameraSequence
        if (entry.frame != nullptr) {
          callback_(channel, midpoint_exposure, entry.frame);
        }
        // output delay message and log
        last_delay_ = midpoint_exposure.toSec() - entry.stamp_camera.toSec();

        geometry_msgs::PointStamped msg;
        msg.header.stamp = midpoint_exposure;
        msg.point.x = last_delay_;
        msg.point.y = entry.seq_camera;
        msg.point.z = seq_trigger;
        delay_pub_.publish(msg);

        ROS_DEBUG_STREAM(
            kLogPrefix << "|cache|= " << cache_queue_[channel].size() - 1 << " Frame w seq_c=" << entry.seq_camera
                       << ", seq_t=" << seq_trigger << " released w t_diff=" << last_delay_);

        // cleanup
        last_published_trigger_seq_[channel]++;
        cache_queue_[channel].erase(it_next_sequence);

      } else {
        ROS_DEBUG_STREAM(kLogPrefix << "|cache|= " << cache_queue_[channel].size() << ". ");
      }

    } while (entry_released);

    // cleanup old entries - warning - special structure of for-loop needed to traverse while removing.
    for (auto it = cache_queue_[channel].cbegin(); it != cache_queue_[channel].cend();/*no inc++*/) {
      if (static_cast<int32_t>(it->first) < last_published_trigger_seq_[channel]) {
        // remove all entries that cannot be published anymore
        cache_queue_[channel].erase(it++);
        ROS_WARN_STREAM(kLogPrefix << "Removing old entries from cache_queue_ - shouldn't happen after startup.");
      } else {
        break;
      }
    }

    // check cache size and issue warning if its large (larger than 1 second worth of messages)
    //   currently there is no bound on cache size in order to not loose anything, maybe change in the future
    if (cache_queue_[channel].size() > static_cast<uint32_t>(framerate_)) {
      ROS_WARN_STREAM(kLogPrefix << "Cache queue too large (" << cache_queue_[channel].size() << " entries)");
    }
  }

  ros::NodeHandle nh_;
  ros::Subscriber cam_imu_sub_;
  ros::Publisher delay_pub_;

  const bool allow_shifted_trigger_seq_;
  const std::set<t_channel_id> channel_set_;
  int trigger_sequence_offset_ = 0;
  std::mutex mutex_;
  caching_callback callback_;
  TriggerState state_;
  std::map<t_channel_id, std::map<uint32_t, cache_queue_type>> cache_queue_;
  std::map<t_channel_id, int32_t> last_published_trigger_seq_;
  double framerate_ = 30.0; // [hz]
  double last_delay_ = 0.0; // [s]
  double static_time_shift_ = 0.0; //[s]
};
}
#endif //REALSENSE2_CAMERA_MAVROS_TIMESTAMP_H

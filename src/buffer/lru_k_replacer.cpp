//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <cstddef>
#include <iostream>
#include <mutex>  // NOLINT
#include "common/config.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> guard(latch_);
  if (curr_size_ == 0) {
    return false;
  }

#define INVALID_FRAME_ID (-1)

  frame_id_t victim = INVALID_FRAME_ID;
  size_t min_timestamp = current_timestamp_ + 1;
  for (auto id : history_list_) {
    if (!frame_info_map_[id].IsEvictable()) {
      continue;
    }
    auto timestamp = frame_info_map_[id].GetTimestamp();
    if (timestamp < min_timestamp) {
      victim = id;
      min_timestamp = timestamp;
    }
  }

  if (victim == INVALID_FRAME_ID) {
    for (auto iter = cache_list_.rbegin(); iter != cache_list_.rend(); iter++) {
      if (!frame_info_map_[*iter].IsEvictable()) {
        continue;
      }
      victim = *iter;
      break;
    }
  }

  if (victim == INVALID_FRAME_ID) {
    return false;
  }

  *frame_id = victim;
  FrameInfo::FrameLocation location = frame_info_map_[victim].GetLocation();
  if (location == FrameInfo::FrameLocation::history) {
    history_list_.remove(victim);
  } else {
    cache_list_.remove(victim);
  }
  frame_info_map_.erase(victim);
  curr_size_--;
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(latch_);
  if (static_cast<size_t>(frame_id) > replacer_size_) {
    std::cerr << "frame_id " << frame_id << " is out of range." << std::endl;
    return;
  }

  IncreseTimestamp();
  if (frame_info_map_.count(frame_id) == 0) {
    frame_info_map_[frame_id] = {frame_id, current_timestamp_};
    history_list_.push_back(frame_id);
    curr_size_++;
  }

  frame_info_map_[frame_id].IncrementAccessCount();

  auto access_count = frame_info_map_[frame_id].GetAccessCount();
  if (access_count == k_) {
    history_list_.remove(frame_id);
    frame_info_map_[frame_id].SetLocation(FrameInfo::FrameLocation::cache);
    cache_list_.push_front(frame_id);
  } else if (access_count > k_) {
    cache_list_.remove(frame_id);
    cache_list_.push_front(frame_id);
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> guard(latch_);
  if (frame_info_map_.count(frame_id) == 0) {
    std::cerr << "frame_id " << frame_id << " not found in frame_info_map_." << std::endl;
    return;
  }

  bool cur_frame_evictable = frame_info_map_[frame_id].IsEvictable();

  if (set_evictable && !cur_frame_evictable) {
    curr_size_++;
  } else if (!set_evictable && cur_frame_evictable) {
    curr_size_--;
  }
  frame_info_map_[frame_id].SetEvictable(set_evictable);
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(latch_);
  if (frame_info_map_.count(frame_id) == 0) {
    return;
  }
  if (!frame_info_map_[frame_id].IsEvictable()) {
    std::cerr << "frame_id " << frame_id << " is not evictable." << std::endl;
    return;
  }
  FrameInfo::FrameLocation location = frame_info_map_[frame_id].GetLocation();
  if (location == FrameInfo::FrameLocation::history) {
    history_list_.remove(frame_id);
  } else {
    cache_list_.remove(frame_id);
  }
  frame_info_map_.erase(frame_id);
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub

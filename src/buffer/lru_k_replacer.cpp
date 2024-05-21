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
#include <algorithm>
#include "common/config.h"
#include "common/exception.h"

namespace bustub {

size_t LRUKNode::CurrentTimeStamp = 0;

LRUKNode::LRUKNode(size_t k, frame_id_t fid) : k_(k), fid_(fid) { record_access(); }

void LRUKNode::record_access() {
  auto current_timestamp_ = static_cast<size_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch())
          .count());

  history_.push_front(current_timestamp_);
  if (history_.size() > k_) {
    history_.pop_back();
  }
}

bool LRUKNode::operator<(const LRUKNode &other) const {
  if (!is_evictable_) {
    return true;
  } else if (!other.is_evictable_) {
    return false;
  }

  auto backward_k_distance_this = backward_k_distance();
  auto backward_k_distance_other = other.backward_k_distance();
  if (backward_k_distance_this == backward_k_distance_other) {
    BUSTUB_ASSERT(history_.size() > 0 && other.history_.size() > 0, "History size should be greater than 0");
    return history_.front() > other.history_.front();
  }

  return backward_k_distance_this < backward_k_distance_other;
}

size_t LRUKNode::backward_k_distance() const {
  if (history_.size() < k_) {
    return std::numeric_limits<size_t>::max();
  }

  return CurrentTimeStamp - history_.back();
}

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  latch_.lock();
  if (curr_size_ == 0) {
    latch_.unlock();
    return false;
  }

  LRUKNode::set_current_timestamp(static_cast<size_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch())
          .count()));
  auto victim = *std::max_element(node_store_.begin(), node_store_.end(),
                                  [](const auto &lhs, const auto &rhs) { return lhs.second < rhs.second; });

  *frame_id = victim.first;
  node_store_.erase(*frame_id);
  curr_size_--;
  latch_.unlock();
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  if (frame_id < 0 || static_cast<size_t>(frame_id) >= replacer_size_) {
    throw Exception("Frame Id is not valid");
  }

  latch_.lock();
  if (node_store_.find(frame_id) == node_store_.end()) {
    node_store_[frame_id] = LRUKNode(k_, frame_id);
  } else {
    node_store_[frame_id].record_access();
  }
  latch_.unlock();
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  latch_.lock();
  if (node_store_.find(frame_id) == node_store_.end()) {
    latch_.unlock();
    throw Exception("Frame Id is not valid");
  }
  bool prev_evictable = node_store_[frame_id].is_evictable();
  if (prev_evictable) {
    if (!set_evictable) {
      curr_size_--;
    }
  } else {
    if (set_evictable) {
      curr_size_++;
    }
  }
  node_store_[frame_id].set_evictable(set_evictable);
  latch_.unlock();
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  latch_.lock();
  if (node_store_.find(frame_id) == node_store_.end()) {
    latch_.unlock();
    return;
  }

  if (!node_store_[frame_id].is_evictable()) {
    latch_.unlock();
    throw Exception("Frame is not evictable");
  }

  node_store_.erase(frame_id);
  curr_size_--;
  latch_.unlock();
}

auto LRUKReplacer::Size() -> size_t {
  latch_.lock();
  size_t size = curr_size_;
  latch_.unlock();
  return size;
}

}  // namespace bustub

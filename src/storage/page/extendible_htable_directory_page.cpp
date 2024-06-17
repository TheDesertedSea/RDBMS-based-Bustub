//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_directory_page.cpp
//
// Identification: src/storage/page/extendible_htable_directory_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_directory_page.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <unordered_map>

#include "common/config.h"
#include "common/logger.h"

namespace bustub {

void ExtendibleHTableDirectoryPage::Init(uint32_t max_depth) {
  memset(local_depths_, 0, sizeof(local_depths_));
  memset(bucket_page_ids_, INVALID_PAGE_ID, sizeof(bucket_page_ids_));
  max_depth_ = max_depth;
  global_depth_ = 0;
}

auto ExtendibleHTableDirectoryPage::HashToBucketIndex(uint32_t hash) const -> uint32_t {
  return hash & ((1 << global_depth_) - 1);
}

auto ExtendibleHTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx) const -> page_id_t {
  return bucket_page_ids_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id) {
  bucket_page_ids_[bucket_idx] = bucket_page_id;
}

auto ExtendibleHTableDirectoryPage::GetSplitImageIndex(uint32_t bucket_idx) const -> uint32_t {
  uint8_t local_depth = local_depths_[bucket_idx];
  return bucket_idx ^ (1 << local_depth);
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepth() const -> uint32_t { return global_depth_; }

void ExtendibleHTableDirectoryPage::IncrGlobalDepth() {
  for (uint32_t i = 0; i < static_cast<uint32_t>(1 << global_depth_); i++) {
    auto counter_idx = i | (1 << global_depth_);
    bucket_page_ids_[counter_idx] = bucket_page_ids_[i];
    local_depths_[counter_idx] = local_depths_[i];
  }
  global_depth_++;
}

void ExtendibleHTableDirectoryPage::DecrGlobalDepth() { global_depth_--; }

auto ExtendibleHTableDirectoryPage::CanShrink() -> bool {
  return std::all_of(local_depths_, local_depths_ + (1 << global_depth_),
                     [this](uint8_t local_depth) { return local_depth < global_depth_; });
}

auto ExtendibleHTableDirectoryPage::Size() const -> uint32_t { return (1 << global_depth_); }

auto ExtendibleHTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) const -> uint32_t {
  return local_depths_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth) {
  local_depths_[bucket_idx] = local_depth;
}

void ExtendibleHTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) { local_depths_[bucket_idx]++; }

void ExtendibleHTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) { local_depths_[bucket_idx]--; }

}  // namespace bustub

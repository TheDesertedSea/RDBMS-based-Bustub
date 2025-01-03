//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_bucket_page.cpp
//
// Identification: src/storage/page/extendible_htable_bucket_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <cstdint>
#include <utility>

#include "storage/page/b_plus_tree_page.h"
#include "storage/page/extendible_htable_bucket_page.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
void ExtendibleHTableBucketPage<KeyType, ValueType, KeyComparator>::Init(uint32_t max_size) {
  size_ = 0;
  max_size_ = max_size;
  std::fill(array_, array_ + max_size_, MappingType(KeyType(), ValueType()));
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Lookup(const K &key, V &value, const KC &cmp) const -> bool {
  for (uint64_t i = 0; i < size_; i++) {
    if (cmp(array_[i].first, key) == 0) {
      value = array_[i].second;
      return true;
    }
  }

  return false;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Insert(const K &key, const V &value, const KC &cmp) -> bool {
  if (IsFull()) {
    return false;
  }

  for (uint32_t i = 0; i < size_; i++) {
    if (cmp(array_[i].first, key) == 0) {
      return false;
    }
  }

  array_[size_++] = std::make_pair(key, value);
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto ExtendibleHTableBucketPage<KeyType, ValueType, KeyComparator>::Remove(const KeyType &key, const KeyComparator &cmp)
    -> bool {
  uint32_t idx = 0;
  for (; idx < size_; idx++) {
    if (cmp(array_[idx].first, key) == 0) {
      break;
    }
  }

  if (idx == size_) {
    return false;
  }

  array_[idx].second = ValueType();
  size_--;

  for (; idx < size_; idx++) {
    array_[idx] = array_[idx + 1];
  }

  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void ExtendibleHTableBucketPage<KeyType, ValueType, KeyComparator>::UpdateAll(
    std::vector<std::pair<KeyType, ValueType>> &entries) {
  size_ = entries.size();
  for (uint32_t i = 0; i < size_; i++) {
    array_[i] = entries[i];
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void ExtendibleHTableBucketPage<KeyType, ValueType, KeyComparator>::RemoveAt(uint32_t bucket_idx) {
  array_[bucket_idx].second = ValueType();
  size_--;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::KeyAt(uint32_t bucket_idx) const -> K {
  return array_[bucket_idx].first;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::ValueAt(uint32_t bucket_idx) const -> V {
  return array_[bucket_idx].second;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::EntryAt(uint32_t bucket_idx) const -> const std::pair<K, V> & {
  return array_[bucket_idx];
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Size() const -> uint32_t {
  return size_;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::IsFull() const -> bool {
  return size_ == max_size_;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::IsEmpty() const -> bool {
  return size_ == 0;
}

template class ExtendibleHTableBucketPage<int, int, IntComparator>;
template class ExtendibleHTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub

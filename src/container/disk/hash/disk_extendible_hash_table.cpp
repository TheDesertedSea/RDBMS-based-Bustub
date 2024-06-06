//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/disk/hash/disk_extendible_hash_table.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "common/util/hash_util.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page_guard.h"
#include "type/value.h"

namespace bustub {

template <typename K, typename V, typename KC>
DiskExtendibleHashTable<K, V, KC>::DiskExtendibleHashTable(std::string name, BufferPoolManager *bpm, const KC &cmp,
                                                           const HashFunction<K> &hash_fn, uint32_t header_max_depth,
                                                           uint32_t directory_max_depth, uint32_t bucket_max_size)
    : index_name_(std::move(name)),
      bpm_(bpm),
      cmp_(cmp),
      hash_fn_(std::move(hash_fn)),
      header_max_depth_(header_max_depth),
      directory_max_depth_(directory_max_depth),
      bucket_max_size_(bucket_max_size) {
  // create the header page
  page_id_t header_page_id;
  auto header_page_guard = bpm_->NewPageGuarded(&header_page_id).UpgradeWrite();
  auto header_page = header_page_guard.AsMut<ExtendibleHTableHeaderPage>();
  header_page->Init(header_max_depth);
  header_page_id_ = header_page_id;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const
    -> bool {
  auto header_page_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_page_guard.As<ExtendibleHTableHeaderPage>();
  auto hash = Hash(key);
  auto directory_idx = header_page->HashToDirectoryIndex(hash);
  page_id_t directory_page_id = header_page->GetDirectoryPageId(directory_idx);
  if (directory_page_id == INVALID_PAGE_ID) {
    return false;
  }

  auto directory_page_guard = bpm_->FetchPageRead(directory_page_id);
  auto directory_page = directory_page_guard.As<ExtendibleHTableDirectoryPage>();
  auto bucket_idx = directory_page->HashToBucketIndex(hash);
  page_id_t bucket_page_id = directory_page->GetBucketPageId(bucket_idx);
  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }

  auto bucket_page_guard = bpm_->FetchPageRead(bucket_page_id);
  auto bucket_page = bucket_page_guard.As<ExtendibleHTableBucketPage<K, V, KC>>();
  V value;
  if (!bucket_page->Lookup(key, value, cmp_)) {
    return false;
  }

  result->push_back(value);
  return true;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

template <typename KeyType, typename ValueType, typename KeyComparator>
auto DiskExtendibleHashTable<KeyType, ValueType, KeyComparator>::Insert(const KeyType &key, const ValueType &value,
                                                                        Transaction *transaction) -> bool {
  auto header_page_guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = header_page_guard.AsMut<ExtendibleHTableHeaderPage>();
  auto hash = Hash(key);
  auto directory_idx = header_page->HashToDirectoryIndex(hash);
  page_id_t directory_page_id = header_page->GetDirectoryPageId(directory_idx);
  if (directory_page_id == INVALID_PAGE_ID) {
    return InsertToNewDirectory(header_page, directory_idx, hash, key, value);
  }

  header_page_guard.Drop();

  auto directory_page_guard = bpm_->FetchPageWrite(directory_page_id);
  auto directory_page = directory_page_guard.AsMut<ExtendibleHTableDirectoryPage>();
  auto bucket_idx = directory_page->HashToBucketIndex(hash);
  page_id_t bucket_page_id = directory_page->GetBucketPageId(bucket_idx);
  if (bucket_page_id == INVALID_PAGE_ID) {
    return InsertToNewBucket(directory_page, bucket_idx, key, value);
  }

  auto bucket_page_guard = bpm_->FetchPageWrite(bucket_page_id);
  auto bucket_page = bucket_page_guard.AsMut<ExtendibleHTableBucketPage<KeyType, ValueType, KeyComparator>>();
  ValueType temp_value;
  auto has_key = bucket_page->Lookup(key, temp_value, cmp_);
  if (has_key) {
    // key already exists
    return false;
  }
  auto insert_result = bucket_page->Insert(key, value, cmp_);
  if (insert_result) {
    return true;
  }
  // bucket is full

  // split the bucket
  auto local_depth = directory_page->GetLocalDepth(bucket_idx);
  auto global_depth = directory_page->GetGlobalDepth();
  if (local_depth == global_depth) {
    if (global_depth == directory_max_depth_) {
      // directory is full
      return false;
    }

    // increase the global depth
    directory_page->IncrGlobalDepth();
  }

  // create a new bucket as the split bucket
  auto split_bucket_idx = directory_page->GetSplitImageIndex(bucket_idx);
  page_id_t split_bucket_page_id;
  auto split_bucket_page_guard = bpm_->NewPageGuarded(&split_bucket_page_id).UpgradeWrite();
  auto split_bucket_page =
      split_bucket_page_guard.AsMut<ExtendibleHTableBucketPage<KeyType, ValueType, KeyComparator>>();
  split_bucket_page->Init(bucket_max_size_);
  // update the directory
  directory_page->SetBucketPageId(split_bucket_idx, split_bucket_page_id);
  directory_page->SetLocalDepth(split_bucket_idx, local_depth + 1);
  directory_page->SetLocalDepth(bucket_idx, local_depth + 1);
  directory_page_guard.Drop();

  // redistribute the keys
  std::vector<MappingType> pairs_original;
  std::vector<MappingType> pairs_split;
  for (size_t i = 0; i < bucket_page->Size(); i++) {
    auto k = bucket_page->KeyAt(i);
    auto v = bucket_page->ValueAt(i);

    if (Hash(k) & (1 << local_depth)) {
      pairs_split.emplace_back(k, v);
    } else {
      pairs_original.emplace_back(k, v);
    }
  }

  bucket_page->UpdateAll(pairs_original);
  split_bucket_page->UpdateAll(pairs_split);

  // insert the new key
  if (Hash(key) & (1 << local_depth)) {
    bucket_page_guard.Drop();
    return split_bucket_page->Insert(key, value, cmp_);
  }

  split_bucket_page_guard.Drop();
  return bucket_page->Insert(key, value, cmp_);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                                                             uint32_t hash, const K &key, const V &value) -> bool {
  page_id_t directory_page_id;
  auto directory_page_guard = bpm_->NewPageGuarded(&directory_page_id).UpgradeWrite();
  auto directory_page = directory_page_guard.AsMut<ExtendibleHTableDirectoryPage>();
  directory_page->Init(directory_max_depth_);
  header->SetDirectoryPageId(directory_idx, directory_page_id);
  return InsertToNewBucket(directory_page, directory_page->HashToBucketIndex(hash), key, value);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool {
  page_id_t bucket_page_id;
  auto bucket_page_guard = bpm_->NewPageGuarded(&bucket_page_id).UpgradeWrite();
  auto bucket_page = bucket_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  bucket_page->Init(bucket_max_size_);
  directory->SetBucketPageId(bucket_idx, bucket_page_id);

  return bucket_page->Insert(key, value, cmp_);
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,
                                                               uint32_t new_bucket_idx, page_id_t new_bucket_page_id,
                                                               uint32_t new_local_depth, uint32_t local_depth_mask) {
  throw NotImplementedException("DiskExtendibleHashTable is not implemented");
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  auto header_page_guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = header_page_guard.AsMut<ExtendibleHTableHeaderPage>();
  auto hash = Hash(key);
  auto directory_idx = header_page->HashToDirectoryIndex(hash);
  page_id_t directory_page_id = header_page->GetDirectoryPageId(directory_idx);
  if (directory_page_id == INVALID_PAGE_ID) {
    return false;
  }
  header_page_guard.Drop();

  auto directory_page_guard = bpm_->FetchPageWrite(directory_page_id);
  auto directory_page = directory_page_guard.AsMut<ExtendibleHTableDirectoryPage>();
  auto bucket_idx = directory_page->HashToBucketIndex(hash);
  page_id_t bucket_page_id = directory_page->GetBucketPageId(bucket_idx);
  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }

  auto bucket_page_guard = bpm_->FetchPageWrite(bucket_page_id);
  auto bucket_page = bucket_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  auto remove_result = bucket_page->Remove(key, cmp_);
  if (!remove_result) {
    return false;
  }

  // try to merge the bucket
  if (directory_page->GetGlobalDepth() == 0) {
    // directory has only one bucket
    return true;
  }

  MergeBuckets(directory_page, bucket_page, std::move(bucket_page_guard), bucket_idx);

  // try to shrink the directory
  while (directory_page->CanShrink()) {
    directory_page->DecrGlobalDepth();
  }

  return true;
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::MergeBuckets(ExtendibleHTableDirectoryPage *directory,
                                                     ExtendibleHTableBucketPage<K, V, KC> *bucket,
                                                     WritePageGuard bucket_guard, uint32_t bucket_idx) {
  // Only empty bucket can start a merge, if the counter bucket is able to merge, the merge should
  // be done by the counter bucket
  if (!bucket->IsEmpty()) {
    return;
  }

  auto local_depth = directory->GetLocalDepth(bucket_idx);
  if (local_depth == 0) {
    // bucket is the only bucket in the directory
    return;
  }

  auto counter_bucket_idx = bucket_idx ^ (1 << (local_depth - 1));
  auto counter_local_depth = directory->GetLocalDepth(counter_bucket_idx);
  if (counter_local_depth != local_depth) {
    return;
  }

  page_id_t counter_bucket_page_id = directory->GetBucketPageId(counter_bucket_idx);
  BUSTUB_ASSERT(counter_bucket_page_id != INVALID_PAGE_ID, "counter bucket page id should not be invalid");

  auto counter_bucket_page_guard = bpm_->FetchPageWrite(counter_bucket_page_id);
  auto counter_bucket_page = counter_bucket_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();

  auto new_local_depth = local_depth - 1;
  auto merged_bucket_idx = std::min(bucket_idx, counter_bucket_idx);
  if (merged_bucket_idx == counter_bucket_idx) {
    // merge the entries from the bucket to the counter bucket
    bpm_->DeletePage(directory->GetBucketPageId(bucket_idx));
    directory->SetBucketPageId(bucket_idx, INVALID_PAGE_ID);
    directory->SetLocalDepth(bucket_idx, 0);
  } else {
    // migrate the entries from the counter bucket to the bucket
    MigrateEntries(counter_bucket_page, bucket);
    bpm_->DeletePage(counter_bucket_page_id);
    directory->SetBucketPageId(counter_bucket_idx, INVALID_PAGE_ID);
    directory->SetLocalDepth(counter_bucket_idx, 0);
  }
  directory->SetLocalDepth(merged_bucket_idx, new_local_depth);

  // try to merge the new merged bucket recursively
  if (merged_bucket_idx == bucket_idx) {
    counter_bucket_page_guard.Drop();
    MergeBuckets(directory, bucket, std::move(bucket_guard), merged_bucket_idx);
  } else {
    bucket_guard.Drop();
    MergeBuckets(directory, counter_bucket_page, std::move(counter_bucket_page_guard), merged_bucket_idx);
  }

  // try to merge the new counter bucket, since it is possible that the merged bucket is not empty
  // but the new counter bucket is empty
  if (new_local_depth == 0) {
    // the new bucket is the only bucket in the directory
    return;
  }

  auto new_counter_bucket_idx = merged_bucket_idx ^ (1 << (new_local_depth - 1));
  auto new_counter_bucket_page_id = directory->GetBucketPageId(new_counter_bucket_idx);
  if (new_counter_bucket_page_id == INVALID_PAGE_ID) {
    return;
  }

  auto new_counter_bucket_page_guard = bpm_->FetchPageWrite(new_counter_bucket_page_id);
  auto new_counter_bucket_page = new_counter_bucket_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  MergeBuckets(directory, new_counter_bucket_page, std::move(new_counter_bucket_page_guard), new_counter_bucket_idx);
}

template <typename KeyType, typename ValueType, typename KC>
void DiskExtendibleHashTable<KeyType, ValueType, KC>::MigrateEntries(
    ExtendibleHTableBucketPage<KeyType, ValueType, KC> *old_bucket,
    ExtendibleHTableBucketPage<KeyType, ValueType, KC> *new_bucket) {
  std::vector<MappingType> entries;
  for (size_t i = 0; i < old_bucket->Size(); i++) {
    entries.emplace_back(old_bucket->EntryAt(i));
  }

  new_bucket->UpdateAll(entries);
  old_bucket->Init(bucket_max_size_);
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub

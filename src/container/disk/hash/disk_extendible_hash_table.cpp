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
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

template <typename K, typename V, typename KC>
DiskExtendibleHashTable<K, V, KC>::DiskExtendibleHashTable(const std::string &name, BufferPoolManager *bpm,
                                                           const KC &cmp, const HashFunction<K> &hash_fn,
                                                           uint32_t header_max_depth, uint32_t directory_max_depth,
                                                           uint32_t bucket_max_size)
    : index_name_(name),
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
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result,
                                                 Transaction *transaction) const -> bool {
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

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  auto header_page_guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = header_page_guard.AsMut<ExtendibleHTableHeaderPage>();
  auto hash = Hash(key);
  auto directory_idx = header_page->HashToDirectoryIndex(hash);
  page_id_t directory_page_id = header_page->GetDirectoryPageId(directory_idx);
  if (directory_page_id == INVALID_PAGE_ID) {
    return InsertToNewDirectory(header_page, directory_idx, hash, key, value);
  }

  auto directory_page_guard = bpm_->FetchPageWrite(directory_page_id);
  auto directory_page = directory_page_guard.AsMut<ExtendibleHTableDirectoryPage>();
  auto bucket_idx = directory_page->HashToBucketIndex(hash);
  page_id_t bucket_page_id = directory_page->GetBucketPageId(bucket_idx);
  if (bucket_page_id == INVALID_PAGE_ID) {
    return InsertToNewBucket(directory_page, bucket_idx, key, value);
  }

  auto bucket_page_guard = bpm_->FetchPageWrite(bucket_page_id);
  auto bucket_page = bucket_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  auto insert_result = bucket_page->Insert(key, value, cmp_);
  if (insert_result) {
    return true;
  }

  if (!bucket_page->IsFull()) {
    // the key is already in the bucket
    return false;
  }

  // split the bucket
  auto local_depth = directory_page->GetLocalDepth(bucket_idx);
  auto global_depth = directory_page->GetGlobalDepth();
  if (local_depth == global_depth) {
    // increase the global depth
    directory_page->IncrGlobalDepth();
  }

  auto split_bucket_idx = directory_page->GetSplitImageIndex(bucket_idx);
  // create a new bucket
  page_id_t split_bucket_page_id;
  auto split_bucket_page_guard = bpm_->NewPageGuarded(&split_bucket_page_id).UpgradeWrite();
  auto split_bucket_page = split_bucket_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  split_bucket_page->Init(bucket_max_size_);
  // update the directory
  directory_page->SetBucketPageId(split_bucket_idx, split_bucket_page_id);
  directory_page->SetLocalDepth(split_bucket_idx, local_depth + 1);
  directory_page->SetLocalDepth(bucket_idx, local_depth + 1);
  // redistribute the keys
  size_t pair_traversed = 0;
  for (size_t i = 0; i < bucket_max_size_; i++) {
    auto k = bucket_page->KeyAt(i);
    auto v = bucket_page->ValueAt(i);
    if (v == V()) {
      continue;
    }

    if (Hash(k) & (1 << local_depth)) {
      bucket_page->RemoveAt(i);
      split_bucket_page->Insert(k, value, cmp_);
    }

    pair_traversed++;
    if (pair_traversed == bucket_page->Size()) {
      break;
    }
  }

  // insert the new key
  if (Hash(key) & (1 << local_depth)) {
    return split_bucket_page->Insert(key, value, cmp_);
  }
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

  if (directory_page->GetGlobalDepth() == 0) {
    return true;
  }

  MergeBuckets(directory_page, bucket_idx, (1 << directory_page->GetLocalDepth(bucket_idx)));

  if (directory_page->CanShrink()) {
    directory_page->DecrGlobalDepth();
  }

  return true;
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::MergeBuckets(ExtendibleHTableDirectoryPage *directory,
                                                     ExtendibleHTableBucketPage<K, V, KC> *bucket, uint32_t bucket_idx,
                                                     uint32_t local_depth) {
  if (!bucket->IsEmpty()) {
    return;
  }

  if (local_depth == 0) {
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

  MergeBuckets(directory, merged_bucket_idx == bucket_idx ? bucket : counter_bucket_page, merged_bucket_idx,
               new_local_depth);
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::MigrateEntries(ExtendibleHTableBucketPage<K, V, KC> *old_bucket,
                                                       ExtendibleHTableBucketPage<K, V, KC> *new_bucket) {
  size_t pair_migrated = 0;
  for (size_t i = 0; i < bucket_max_size_; i++) {
    auto k = old_bucket->KeyAt(i);
    auto v = old_bucket->ValueAt(i);
    if (v == V()) {
      continue;
    }

    old_bucket->RemoveAt(i);
    new_bucket->Insert(k, v, cmp_);
    pair_migrated++;
    if (pair_migrated == old_bucket->Size()) {
      break;
    }
  }
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub

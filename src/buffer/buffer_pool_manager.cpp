//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"
#include <mutex>

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"
#include "common/macros.h"
#include "storage/disk/disk_scheduler.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::scoped_lock latch(latch_);
  int frame_id = -1;
  Page *page = nullptr;
  if (free_list_.empty()) {
    // No free page
    auto evict_result = replacer_->Evict(&frame_id);
    if (!evict_result) {
      // No evictable page
      return nullptr;
    }

    page = &pages_[frame_id];
    if (page->IsDirty()) {
      // Write the previous page back to disk
      auto promise = disk_scheduler_->CreatePromise();
      auto future = promise.get_future();
      disk_scheduler_->Schedule({true, page->GetData(), page->GetPageId(), std::move(promise)});
      future.get();
    }
    page_table_.erase(page->GetPageId());
    page->ResetMemory();
    page->page_id_ = INVALID_PAGE_ID;
    page->pin_count_ = 0;
    page->is_dirty_ = false;
  } else {
    // Get a page from the free list
    frame_id = free_list_.front();
    free_list_.pop_front();
    page = &pages_[frame_id];
  }

  *page_id = AllocatePage();
  page->page_id_ = *page_id;
  page->pin_count_ = 1;
  replacer_->RecordAccess(frame_id, AccessType::Unknown);  // Add the frame to the replacer
  replacer_->SetEvictable(frame_id, false);                // The page is pinned
  page_table_[*page_id] = frame_id;
  return page;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::scoped_lock latch(latch_);
  int frame_id = -1;
  Page *page = nullptr;
  if (page_table_.find(page_id) == page_table_.end()) {
    // Page not in the buffer pool
    if (free_list_.empty()) {
      // No free page
      auto evict_result = replacer_->Evict(&frame_id);
      if (!evict_result) {
        // No evictable page
        return nullptr;
      }

      page = &pages_[frame_id];
      if (page->IsDirty()) {
        // Write the previous page back to disk
        auto promise = disk_scheduler_->CreatePromise();
        auto future = promise.get_future();
        disk_scheduler_->Schedule({true, page->GetData(), page->GetPageId(), std::move(promise)});
        future.get();
      }
      page_table_.erase(page->GetPageId());
      page->ResetMemory();
      page->page_id_ = INVALID_PAGE_ID;
      page->pin_count_ = 0;
      page->is_dirty_ = false;
    } else {
      frame_id = free_list_.front();
      free_list_.pop_front();
      page = &pages_[frame_id];
    }

    page_table_[page_id] = frame_id;
    page->page_id_ = page_id;

    // Read page from disk
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    disk_scheduler_->Schedule({false, page->GetData(), page->GetPageId(), std::move(promise)});
    future.get();
  } else {
    // Get the page from the buffer pool
    frame_id = page_table_[page_id];
    page = &pages_[frame_id];
  }

  page->pin_count_++;
  replacer_->RecordAccess(frame_id, access_type);
  replacer_->SetEvictable(frame_id, false);  // The page is pinned
  return page;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::scoped_lock latch(latch_);
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }

  int frame_id = page_table_[page_id];
  Page *page = &pages_[frame_id];
  if (page->pin_count_ <= 0) {
    return false;
  }

  page->pin_count_--;
  if (is_dirty) {
    page->is_dirty_ = is_dirty;
  }
  if (page->pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::scoped_lock latch(latch_);
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }

  int frame_id = page_table_[page_id];
  Page *page = &pages_[frame_id];
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  disk_scheduler_->Schedule({true, page->GetData(), page->GetPageId(), std::move(promise)});
  future.get();
  page->is_dirty_ = false;
  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::scoped_lock latch(latch_);
  for (auto &entry : page_table_) {
    page_id_t page_id = entry.first;
    int frame_id = entry.second;
    Page *page = &pages_[frame_id];
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    disk_scheduler_->Schedule({true, page->GetData(), page_id, std::move(promise)});
    future.get();
    page->is_dirty_ = false;
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::scoped_lock latch(latch_);
  if (page_table_.find(page_id) == page_table_.end()) {
    return true;
  }

  int frame_id = page_table_[page_id];
  Page *page = &pages_[frame_id];
  if (page->pin_count_ > 0) {
    return false;
  }

  if (page->IsDirty()) {
    // Write the previous page back to disk
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    disk_scheduler_->Schedule({true, page->GetData(), page->GetPageId(), std::move(promise)});
    future.get();
  }

  page_table_.erase(page_id);
  replacer_->Remove(frame_id);
  free_list_.push_back(frame_id);
  page->ResetMemory();
  page->page_id_ = INVALID_PAGE_ID;
  page->pin_count_ = 0;
  page->is_dirty_ = false;
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  Page *page = FetchPage(page_id, AccessType::Unknown);
  return {this, page};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  Page *page = FetchPage(page_id, AccessType::Unknown);
  page->RLatch();
  return {this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  Page *page = FetchPage(page_id, AccessType::Unknown);
  page->WLatch();
  return {this, page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  Page *page = NewPage(page_id);
  return {this, page};
}

}  // namespace bustub

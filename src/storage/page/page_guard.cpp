#include "storage/page/page_guard.h"
#include <iostream>
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  std::cout << "BasicPageGuard(BasicPageGuard &&that), page id: " << that.page_->GetPageId() << std::endl;

  bpm_ = that.bpm_;
  page_ = that.page_;
  is_dirty_ = that.is_dirty_;
  that.bpm_ = nullptr;
  that.page_ = nullptr;
  that.is_dirty_ = false;
}

void BasicPageGuard::Drop() {
  if (bpm_ == nullptr || page_ == nullptr) {
    std::cout << "BasicPageGuard::Drop(), bpm_ == nullptr || page_ == nullptr" << std::endl;
    return;
  }

  std::cout << "BasicPageGuard::Drop(), page id: " << page_->GetPageId() << std::endl;

  bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
  bpm_ = nullptr;
  page_ = nullptr;
  is_dirty_ = false;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  if (this == &that) {
    return *this;
  }

  std::cout << "BasicPageGuard::operator=(BasicPageGuard &&that), page id: " << that.page_->GetPageId() << std::endl;

  Drop();

  bpm_ = that.bpm_;
  page_ = that.page_;
  is_dirty_ = that.is_dirty_;
  that.bpm_ = nullptr;
  that.page_ = nullptr;
  that.is_dirty_ = false;
  return *this;
}

BasicPageGuard::~BasicPageGuard() {
  std::cout << "BasicPageGuard::~BasicPageGuard()" << std::endl;  // NOLINT
  Drop();
};  // NOLINT

auto BasicPageGuard::UpgradeRead() -> ReadPageGuard {
  std::cout << "BasicPageGuard::UpgradeRead(), page id: " << page_->GetPageId() << std::endl;

  ReadPageGuard read_page_guard(this->bpm_, this->page_);
  this->page_->RLatch();
  this->bpm_ = nullptr;
  this->page_ = nullptr;
  return read_page_guard;
}

auto BasicPageGuard::UpgradeWrite() -> WritePageGuard {
  std::cout << "BasicPageGuard::UpgradeWrite(), page id: " << page_->GetPageId() << std::endl;

  WritePageGuard write_page_guard(this->bpm_, this->page_);
  this->page_->WLatch();
  this->bpm_ = nullptr;
  this->page_ = nullptr;
  return write_page_guard;
}

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept { guard_ = std::move(that.guard_); }

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  if (this == &that) {
    return *this;
  }

  std::cout << "ReadPageGuard::operator=(ReadPageGuard &&that), page id: " << that.guard_.page_->GetPageId()
            << std::endl;

  Drop();

  guard_ = std::move(that.guard_);
  return *this;
}

void ReadPageGuard::Drop() {
  if (guard_.bpm_ == nullptr || guard_.page_ == nullptr) {
    std::cout << "ReadPageGuard::Drop(), guard_.bpm_ == nullptr || guard_.page_ == nullptr" << std::endl;
    return;
  }

  std::cout << "ReadPageGuard::Drop(), page id: " << guard_.page_->GetPageId() << std::endl;

  guard_.page_->RUnlatch();
  guard_.Drop();
}

ReadPageGuard::~ReadPageGuard() {
  std::cout << "ReadPageGuard::~ReadPageGuard()" << std::endl;  // NOLINT
  Drop();
}  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept {
  std::cout << "WritePageGuard(WritePageGuard &&that), page id: " << that.guard_.page_->GetPageId() << std::endl;
  guard_ = std::move(that.guard_);
}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (this == &that) {
    return *this;
  }

  std::cout << "WritePageGuard::operator=(WritePageGuard &&that), page id: " << that.guard_.page_->GetPageId()
            << std::endl;

  Drop();

  guard_ = std::move(that.guard_);
  return *this;
}

void WritePageGuard::Drop() {
  if (guard_.bpm_ == nullptr || guard_.page_ == nullptr) {
    std::cout << "WritePageGuard::Drop(), guard_.bpm_ == nullptr || guard_.page_ == nullptr" << std::endl;
    return;
  }

  std::cout << "WritePageGuard::Drop(), page id: " << guard_.page_->GetPageId() << std::endl;

  guard_.page_->WUnlatch();
  guard_.Drop();
}

WritePageGuard::~WritePageGuard() {
  std::cout << "WritePageGuard::~WritePageGuard()" << std::endl;  // NOLINT
  Drop();
}  // NOLINT

}  // namespace bustub

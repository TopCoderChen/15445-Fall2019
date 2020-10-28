//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new ClockReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

// Page is always there in memory, just need to change its inner page_id_ & data_ & metadata.
Page *BufferPoolManager::ReplaceAndUpdate(page_id_t new_page_id, bool new_page,
                                          std::unique_lock<std::shared_mutex> *u_lock) {
  assert(!free_list_.empty() || replacer_->Size() != 0);
  Page *page;
  frame_id_t index;
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  if (!free_list_.empty()) {
    index = free_list_.front();
    free_list_.pop_front();
    page_table_.emplace(new_page_id, index);
    page = pages_ + index;
    page->WLatch();
    u_lock->unlock();
    if (!new_page) {
      disk_manager_->ReadPage(new_page_id, page->data_);
    }
  } else {  // replacer
    [[maybe_unused]] bool victim_res = replacer_->Victim(&index);
    page = pages_ + index;
    page_table_.erase(page->page_id_);
    page_table_.emplace(new_page_id, index);
    replacer_->Pin(index);
    page->WLatch();
    u_lock->unlock();
    // 2.     If R is dirty, write it back to the disk.
    if (page->is_dirty_) {
      disk_manager_->WritePage(page->page_id_, page->data_);
    }
    if (!new_page) {
      disk_manager_->ReadPage(new_page_id, page->data_);
    } else {
      // zero out memory
      page->ResetMemory();
    }
  }
  // 4.     Update P's metadata,
  page->page_id_ = new_page_id;
  page->pin_count_ = 1;
  page->is_dirty_ = new_page;
  page->WUnlatch();
  return page;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  std::unique_lock lock(latch_);
  auto iter = page_table_.find(page_id);

  // If P exists, pin it and return it immediately.
  if (iter != page_table_.end()) {
    // find frame index by page_id
    auto index = iter->second;
    auto *page = pages_ + index;
    page->pin_count_++;
    if (page->pin_count_ > 0) {
      replacer_->Pin(index);
    }
    return page;
  }

  if (free_list_.empty() && replacer_->Size() == 0) {
    return nullptr;
  }
  return ReplaceAndUpdate(page_id, false, &lock);
}

// * @param is_dirty true if the page should be marked as dirty, false otherwise
// * @return false if the page pin count is <= 0 before this call, true otherwise
bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  std::unique_lock u_lock(latch_);
  const auto got = page_table_.find(page_id);
  auto frame_index = got->second;
  auto* page = pages_ + frame_index;
  if (page->pin_count_ <= 0) {
    return false;
  }
  if (--page->pin_count_ == 0) {
    replacer_->Unpin(got->second);
  }
  page->is_dirty_ |= is_dirty;
  return true;
}

// Flushes the target page to disk.
bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  std::unique_lock lock(latch_);
  const auto& got = page_table_.find(page_id);
  if (got == page_table_.end()) {
    lock.unlock();
    return false;
  }
  auto *page = pages_ + got->second;
  page->WLatch();
  lock.unlock();
  if (page->page_id_ != INVALID_PAGE_ID && page->is_dirty_) {
    page->is_dirty_ = false;
    disk_manager_->WritePage(page->page_id_, page->data_);
  }
  page->WUnlatch();
  return true;
}

// @return nullptr if no new pages could be created, otherwise pointer to new page
Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  std::unique_lock lock(latch_);
  if (free_list_.empty() && replacer_->Size() == 0) {
    *page_id = INVALID_PAGE_ID;
    return nullptr;
  }
  page_id_t id = disk_manager_->AllocatePage();
  *page_id = id;
  return ReplaceAndUpdate(id, true, &lock);
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  std::unique_lock u_lock(latch_);
  const auto &got = page_table_.find(page_id);
  if (got == page_table_.end()) {
    u_lock.unlock();
    disk_manager_->DeallocatePage(page_id);
    return true;
  }
  auto index = got->second;
  auto* page = pages_ + index;
  page->WLatch();
  // 2. If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  if (page->pin_count_ > 0) {
    u_lock.unlock();
    page->WUnlatch();
    return false;
  }
  // 3.   Otherwise, P can be deleted. Remove P from the page table,
  replacer_->Pin(index);
  page_table_.erase(got->first);
  free_list_.emplace_back(index);
  u_lock.unlock();

  // reset its metadata and return it to the free list.
  disk_manager_->DeallocatePage(page_id);
  page->ResetMemory();
  page->page_id_ = INVALID_PAGE_ID;
  page->is_dirty_ = false;
  page->WUnlatch();

  return false;
}

// Flush all pages to disk. Actually only need to flush valid dirty pages.
void BufferPoolManager::FlushAllPagesImpl() {
  std::unique_lock lock(latch_);
  for (size_t i = 0; i < pool_size_; i++) {
    auto* page = pages_ + i;
    if (page->page_id_ != INVALID_PAGE_ID && page->is_dirty_) {
      disk_manager_->WritePage(page->page_id_, page->data_);
      page->is_dirty_ = false;
    }
  }
}

}  // namespace bustub

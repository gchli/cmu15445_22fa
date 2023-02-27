//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"
#include <mutex>  // NOLINT
#include <string>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::lock_guard<std::mutex> guard(latch_);
  frame_id_t frame_id;
  Page *cur_page = GetOnePage(frame_id);

  if (cur_page == nullptr) {
    return nullptr;
  }

  *page_id = AllocatePage();
  cur_page->page_id_ = *page_id;
  cur_page->pin_count_ = 1;
  cur_page->is_dirty_ = false;
  cur_page->ResetMemory();
  page_table_->Insert(*page_id, frame_id);
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  // if (*page_id == 3) {
  //   std::cout << "page id: " << *page_id << " init." << std::endl;
  // }
  return cur_page;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::lock_guard<std::mutex> guard(latch_);
  frame_id_t frame_id = -1;
  Page *cur_page = nullptr;
  // if (page_id == 3) {
  //   std::cout << "page id: " << page_id << " fetching." << std::endl;
  // }
  if (page_table_->Find(page_id, frame_id)) {
    cur_page = &pages_[frame_id];
    cur_page->pin_count_++;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    return cur_page;
  }

  bool all_used = true;
  for (size_t i = 0; i < pool_size_; ++i) {
    if (pages_[i].GetPinCount() <= 0) {
      all_used = false;
      break;
    }
  }
  if (all_used) {
    return nullptr;
  }

  cur_page = GetOnePage(frame_id);
  if (cur_page == nullptr) {
    return nullptr;
  }

  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  page_table_->Insert(page_id, frame_id);

  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].pin_count_ = 1;
  pages_[frame_id].is_dirty_ = false;
  disk_manager_->ReadPage(page_id, cur_page->GetData());
  return cur_page;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::lock_guard<std::mutex> guard(latch_);
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }
  Page *cur_page = &pages_[frame_id];

  // if (page_id == 3) {
  //   std::cout << "page id: " << page_id << " unpining." << std::endl;
  // }
  if (cur_page->GetPinCount() <= 0) {
    std::cout << "page id: " << page_id << " pin count lt 0." << std::endl;
  }
  assert(cur_page->GetPinCount() > 0);

  cur_page->pin_count_--;
  if (is_dirty) {
    cur_page->is_dirty_ = true;
  }
  if (cur_page->pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> guard(latch_);
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }
  Page *cur_page = &pages_[frame_id];
  disk_manager_->WritePage(page_id, cur_page->GetData());
  cur_page->is_dirty_ = false;
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  for (size_t i = 0; i < pool_size_; ++i) {
    if (pages_[i].IsDirty()) {
      FlushPgImp(pages_[i].GetPageId());
    }
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> guard(latch_);
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return true;
  }
  Page *cur_page = &pages_[frame_id];
  if (cur_page->GetPinCount() > 0) {
    std::cout << "page id: " << page_id << " pin count > 0." << std::endl;
  }
  assert(cur_page->GetPinCount() == 0);

  if (cur_page->IsDirty()) {
    disk_manager_->WritePage(cur_page->GetPageId(), cur_page->GetData());
  }
  replacer_->Remove(frame_id);
  page_table_->Remove(page_id);
  cur_page->ResetMemory();
  cur_page->page_id_ = INVALID_PAGE_ID;
  cur_page->is_dirty_ = false;
  cur_page->pin_count_ = 0;
  free_list_.emplace_back(frame_id);
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManagerInstance::GetOnePage(frame_id_t &frame_id) -> Page * {
  // std::lock_guard<std::mutex> guard(latch_);
  Page *cur_page = nullptr;

  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    cur_page = &pages_[frame_id];
    return cur_page;
  }

  if (!replacer_->Evict(&frame_id)) {
    return nullptr;
  }

  cur_page = &pages_[frame_id];
  if (cur_page->IsDirty()) {
    disk_manager_->WritePage(cur_page->GetPageId(), cur_page->GetData());
  }
  page_table_->Remove(cur_page->GetPageId());
  return cur_page;
}
}  // namespace bustub

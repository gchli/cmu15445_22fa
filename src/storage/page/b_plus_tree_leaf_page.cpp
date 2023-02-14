//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/config.h"
#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "type/value.h"
#include "common/logger.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetSize(0);
  SetMaxSize(max_size - 1);
  SetPageType(IndexPageType::LEAF_PAGE);
  SetNextPageId(INVALID_PAGE_ID);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  assert(index >= 0 && index < GetSize());
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  // replace with your own code
  assert(index >= 0 && index < GetSize());
  return array_[index].second;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) -> int {
  // replace with your own code
  int idx = IndexOf(key, comparator);
  int cur_size = GetSize();
  assert(idx <= cur_size);

  if (idx < cur_size && comparator(KeyAt(idx), key) == 0) {
    // TODO(ligch): Recheck this. Update or do nothing?
    array_[idx].second = value;
  } else {
    // TODO(ligch): memcpy maybe more faster?
    for (int i = cur_size; i > idx; i--) {
      array_[i].first = array_[i - 1].first;
      array_[i].second = array_[i - 1].second;
    }
    array_[idx].first = key;
    array_[idx].second = value;
    IncreaseSize(1);
  }

  return GetSize();
}

/* Find the value of the corresponding key. If not exists, return false. */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Find(const KeyType &key, ValueType &value, const KeyComparator &comparator) const -> bool {
  int idx = IndexOf(key, comparator);
  if (idx >= GetSize() || comparator(KeyAt(idx), key) != 0) {
    return false;
  }
  value = ValueAt(idx);
  return true;
}


INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::RedistributeLeafPage(B_PLUS_TREE_LEAF_PAGE_TYPE *to_page, BufferPoolManager *buffer_pool_manager) -> void {
  int total_size = GetSize();
  assert(total_size == GetMaxSize() + 1);
  int idx = total_size / 2;
  // TODO(ligch): Using memcpy() instead?
  // why to_page->array_ isn't private here?
  for (int i = idx; i < total_size; i++) {
    to_page->array_[i - idx].first = array_[i].first;
    to_page->array_[i - idx].second = array_[i].second;
  }
  to_page->SetSize(total_size - idx);
  SetSize(idx);
}

/* array_[l]'s key is larger than or equal to the parameter key. */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::IndexOf(const KeyType &key, const KeyComparator &comparator) const -> int {
  // replace with your own code
  int l = 0;
  int r = GetSize();
  while (l < r) {
    /*If # of elements is even, choose the left one as middle.
      To prevent infinite loop, we can't assign mid to left below. */
    int mid = (r - l) / 2 + l;
    if (comparator(array_[mid].first, key) < 0) {
      l = mid + 1;
    } else {
      r = mid;
    }
  }
  return l;
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Remove(const KeyType &key, ValueType &value, const KeyComparator &comparator) -> bool {
  int idx = IndexOf(key, comparator);
  if (idx >= GetSize() || comparator(KeyAt(idx), key) != 0) {
    return false;
  }
  value = ValueAt(idx);
  for (int i = idx; i < GetSize() - 1; i++) {
    array_[i].first = array_[i + 1].first;
    array_[i].second = array_[i + 1].second;
  }
  IncreaseSize(-1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveFront() -> void {
  for (int i = 0; i < GetSize() - 1; i++) {
    array_[i].first = array_[i + 1].first;
    array_[i].second = array_[i + 1].second;
  }
  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::CopyAllFrom(BPlusTreeLeafPage *leaf_page) -> void {
  int ori_size = GetSize();
  int new_size = ori_size + leaf_page->GetSize();
  SetSize(new_size);
  leaf_page->SetSize(0);
  for (int i = ori_size; i < new_size; i++) {
    array_[i].first = leaf_page->KeyAt(i - ori_size);
    array_[i].second = leaf_page->ValueAt(i - ori_size);
  }
}


template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub

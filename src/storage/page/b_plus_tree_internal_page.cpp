//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetSize(0);
  SetMaxSize(max_size - 1);
  SetPageType(IndexPageType::INTERNAL_PAGE);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  assert(index >= 0 && index < GetSize());
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { 
  assert(index >= 0 && index < GetSize());
  array_[index].first = key;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  assert(index >= 0 && index < GetSize());
  return array_[index].second;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) {
  assert(index >= 0 && index < GetSize());
  array_[index].second = value;
}

/* array_[l]'s key is larger than the parameter key. arrary[l - 1]'s value may contain the key.
   So return l - 1.
   array_[0] only has value, so the binary search's left boundary should start from 1. */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::IndexOf(const KeyType &key, const KeyComparator &comparator) const -> int {
  int l = 1;
  int r = GetSize();
  while (l < r) {
    int mid = (r - l) / 2 + l;
    if (comparator(array_[mid].first, key) <= 0) {
      l = mid + 1;
    } else {
      r = mid;
    }
  }
  return l - 1;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetupNewRoot(BPlusTreePage* old_page, const KeyType &key, BPlusTreePage* new_page) -> void {
  SetSize(2);
  SetValueAt(0, old_page->GetPageId());
  SetKeyAt(1, key);
  SetValueAt(1, new_page->GetPageId());
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::RedistributeInternalPage(B_PLUS_TREE_INTERNAL_PAGE_TYPE *to_page, BufferPoolManager *buffer_pool_manager) -> void {
  //TODO(ligch): Maybe this function can be reconstruected.
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

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) -> int {
  // replace with your own code

  // For insert, the kv should insert after the idx.
  int idx = IndexOf(key, comparator) + 1;
  int cur_size = GetSize();
  assert(idx <= cur_size);

  if (idx < cur_size && comparator(KeyAt(idx), key) == 0) {
    // TODO(ligch): Recheck this. Update or do nothing?
    std::cerr << "duplicated key" << std::endl;
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

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub

//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/b_plus_tree.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <queue>
#include <string>
#include <utility>
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "concurrency/transaction.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {

#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator>

enum class OpType { FIND, INSERT, REMOVE };
/**
 * Main class providing the API for the Interactive B+ Tree.
 *
 * Implementation of simple b+ tree data structure where internal pages direct
 * the search and leaf pages contain actual data.
 * (1) We only support unique key
 * (2) support insert & remove
 * (3) The structure should shrink and grow dynamically
 * (4) Implement index iterator for range scan
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTree {
  using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  explicit BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                     int leaf_max_size = LEAF_PAGE_SIZE, int internal_max_size = INTERNAL_PAGE_SIZE);

  // Returns true if this B+ tree has no keys and values.
  auto IsEmpty() const -> bool;

  // Insert a key-value pair into this B+ tree.
  auto Insert(const KeyType &key, const ValueType &value, Transaction *transaction = nullptr) -> bool;

  // Remove a key and its value from this B+ tree.
  void Remove(const KeyType &key, Transaction *transaction = nullptr);

  // return the value associated with a given key
  auto GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction = nullptr) -> bool;

  // return the page id of the root node
  auto GetRootPageId() -> page_id_t;

  // index iterator
  auto Begin() -> INDEXITERATOR_TYPE;
  auto Begin(const KeyType &key) -> INDEXITERATOR_TYPE;
  auto End() -> INDEXITERATOR_TYPE;

  // print the B+ tree
  void Print(BufferPoolManager *bpm);

  // draw the B+ tree
  void Draw(BufferPoolManager *bpm, const std::string &outf);

  // read data from file and insert one by one
  void InsertFromFile(const std::string &file_name, Transaction *transaction = nullptr);

  // read data from file and remove one by one
  void RemoveFromFile(const std::string &file_name, Transaction *transaction = nullptr);

 private:
  auto FindLeafPage(const KeyType &key, OpType op_type, Transaction *transaction) -> LeafPage *;
  auto FetchTreePage(page_id_t page_id) -> BPlusTreePage *;
  auto FetchLeafPage(page_id_t page_id) -> LeafPage *;
  auto FetchPage(page_id_t page_id) -> Page *;
  auto ToTreePage(Page *page) -> BPlusTreePage *;
  auto ToLeafPage(Page *page) -> LeafPage *;
  auto ToInternalPage(Page *page) -> InternalPage *;
  auto ToLeafPage(BPlusTreePage *tree_page) -> LeafPage *;
  auto ToInternalPage(BPlusTreePage *tree_page) -> InternalPage *;
  auto FetchInternalPage(page_id_t page_id) -> InternalPage *;

  auto SplitLeafPage(LeafPage *leaf_page, BufferPoolManager *buffer_pool_manager, Transaction *transaction)
      -> LeafPage *;
  auto SplitInternalPage(InternalPage *internal_page, std::pair<KeyType, page_id_t> &new_item,
                         BufferPoolManager *buffer_pool_manager, Transaction *transaction) -> InternalPage *;
  auto InsertInParent(BPlusTreePage *old_page, const KeyType &key, BPlusTreePage *new_page, Transaction *transaction)
      -> void;
  void UpdateRootPageId(int insert_record = 0);
  void CreateNewRoot(const KeyType &key, const ValueType &value, Transaction *transaction);
  void DeleteEntry(BPlusTreePage *page, const KeyType &key, Transaction *transaction);

  auto CanCoalesce(BPlusTreePage *page, page_id_t &l_page_id, page_id_t &r_page_id, Transaction *transaction) -> bool;
  auto CanRedistribute(BPlusTreePage *page, int &loc, page_id_t &from_page, Transaction *transaction) -> bool;
  void DoCoalesce(BPlusTreePage *left_page, BPlusTreePage *right_page, const KeyType &key, Transaction *transaction);
  void DoRedistribute(BPlusTreePage *page, int &loc, BPlusTreePage *from_page, const KeyType &key,
                      Transaction *transaction);
  auto FindLeftmostLeafPage() -> LeafPage *;
  auto FindRightmostLeafPage() -> LeafPage *;

  void LockTreeRoot(OpType op_type, Transaction *transaction);
  void UnlockTreeRoot(OpType op_type);
  void UnlatchAllPages(Transaction *transaction, OpType op_type, bool dirty);
  void DeleteAllPages(Transaction *transaction);
  auto SafeToUnlatchAll(Transaction *transaction, OpType op_type, BPlusTreePage *tree_page) -> bool;

  /* Debug Routines for FREE!! */
  void ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const;

  void ToString(BPlusTreePage *page, BufferPoolManager *bpm) const;

  // member variable
  std::string index_name_;
  page_id_t root_page_id_;
  BufferPoolManager *buffer_pool_manager_;
  KeyComparator comparator_;
  ReaderWriterLatch tree_latch_;
  int leaf_max_size_;
  int internal_max_size_;
};

}  // namespace bustub

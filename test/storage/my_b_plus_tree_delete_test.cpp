/**
 * b_plus_tree_delete_test.cpp
 */

#include <algorithm>
#include <cstdio>
#include <random>
#include <string>

#include "buffer/buffer_pool_manager_instance.h"
#include "gtest/gtest.h"
#include "storage/index/b_plus_tree.h"
#include "test_util.h"  // NOLINT

namespace bustub {

TEST(MyBPlusTreeTests, DeleteTest1) {
  // create KeyComparator and index schema

  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 2, 3);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto transaction = new Transaction(0);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }

  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  int64_t start_key = 1;
  int64_t current_key = start_key;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }

  EXPECT_EQ(current_key, keys.size() + 1);
  tree.Draw(bpm, "before_remove.dot");
  std::vector<int64_t> remove_keys = {1, 5};
  for (auto key : remove_keys) {
    index_key.SetFromInteger(key);
    tree.Remove(index_key, transaction);
  }

  start_key = 2;
  current_key = start_key;
  int64_t size = 0;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
    size = size + 1;
  }

  EXPECT_EQ(size, 3);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(MyBPlusTreeTests, DeleteTest2) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 2, 3);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto transaction = new Transaction(0);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }
  tree.Draw(bpm, "after_insert.dot");

  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  int64_t start_key = 1;
  int64_t current_key = start_key;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }

  EXPECT_EQ(current_key, keys.size() + 1);
  std::vector<int64_t> remove_keys = {1, 5, 3, 4};
  for (auto key : remove_keys) {
    index_key.SetFromInteger(key);
    tree.Remove(index_key, transaction);
    tree.Draw(bpm, "after_remove" + std::to_string(key) + ".dot");
  }

  start_key = 2;
  current_key = start_key;
  int64_t size = 0;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
    size = size + 1;
  }

  EXPECT_EQ(size, 1);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(MyBPlusTreeTests, DISABLED_DeleteTest3) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 3, 4);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto transaction = new Transaction(0);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  std::vector<int64_t> keys = {'S', 'W', 'M', 'N', 'K', 'G', 'F', 'E', 'C', 'D', 'B', 'L', 'A'};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }

  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  std::vector<char> remove_keys = {'S', 'N', 'W', 'G'};
  for (auto key : remove_keys) {
    index_key.SetFromInteger(key);
    tree.Remove(index_key, transaction);
  }

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(MyBPlusTreeTests, DISABLED_DeleteTest4) {
  int loop = 1;
  for (int test = 0; test < loop; ++test) {
    // create KeyComparator and index schema
    auto key_schema = ParseCreateStatement("a bigint");
    GenericComparator<64> comparator(key_schema.get());

    auto disk_manager = new DiskManager("test.db");
    BufferPoolManager *bpm = new BufferPoolManagerInstance(30, disk_manager);
    // create b+ tree
    BPlusTree<GenericKey<64>, RID, GenericComparator<64>> tree("foo_pk", bpm, comparator, 2, 3);
    GenericKey<64> index_key;
    RID rid;
    // create transaction
    auto transaction = new Transaction(0);

    // create and fetch header_page
    page_id_t page_id;
    auto header_page = bpm->NewPage(&page_id);
    (void)header_page;

    int scale = 10;  // at first, set a small number(6-10) to find bug
    std::vector<int64_t> keys(scale);
    for (int i = 0; i < scale; ++i) {
      keys[i] = i + 1;
    }
    std::shuffle(keys.begin(), keys.end(), std::mt19937(std::random_device()()));
    // std::cout << "\n--- to insert: ";
    // for (auto key : keys) {
    //   std::cout << key << ' ';
    // }
    // std::cout << '\n';
    std::vector<int64_t> c_keys = keys;
    std::reverse(c_keys.begin(), c_keys.end());
    for (auto key : keys) {
      int64_t value = key & 0xFFFFFFFF;
      rid.Set(static_cast<int32_t>(key >> 32), value);
      index_key.SetFromInteger(key);
      std::cout << key << ' ';
      tree.Insert(index_key, rid, transaction);
      // tree.Draw(bpm, "after_insert" + std::to_string(key) + ".dot");
    }
    tree.Draw(bpm, "after_insert.dot");
    std::cout << "--- insert end, delete start ---\n";
    for (auto key : keys) {
      index_key.SetFromInteger(key);
      // std::cout << "remove: " << key << '\n';
      tree.Remove(index_key, transaction);
      // tree.Draw(bpm, "after_remove" + std::to_string(key) + ".dot");
      c_keys.pop_back();
      std::vector<RID> rids;
      // check key & value pairs
      // std::cout << "checking remaining key & value ";
      for (auto key2 : c_keys) {
        rids.clear();
        index_key.SetFromInteger(key2);
        tree.GetValue(index_key, &rids);
        EXPECT_EQ(rids.size(), 1);

        int64_t value = key2 & 0xFFFFFFFF;
        EXPECT_EQ(rids[0].GetSlotNum(), value);
      }
      // std::cout << " -> ok\n";
    }
    bpm->UnpinPage(HEADER_PAGE_ID, true);
    delete transaction;
    delete disk_manager;
    delete bpm;
    remove("test.db");
    remove("test.log");
  }
}
TEST(MyBPlusTreeTests, DeleteTest5) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<64> comparator(key_schema.get());

  auto disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(30, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<64>, RID, GenericComparator<64>> tree("foo_pk", bpm, comparator, 2, 3);
  GenericKey<64> index_key;
  RID rid;
  // create transaction
  auto transaction = new Transaction(0);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  int scale = 1000;  // at first, set a small number(6-10) to find bug
  std::vector<int64_t> keys(scale);
  for (int i = 0; i < scale; ++i) {
    keys[i] = i + 1;
  }
  std::shuffle(keys.begin(), keys.end(), std::mt19937(std::random_device()()));

  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }
  // std::shuffle(keys.begin(), keys.end(), std::mt19937(std::random_device()()));

  std::vector<int64_t> c_keys = keys;
  std::reverse(c_keys.begin(), c_keys.end());
  int counter = 0;
  for (auto key : keys) {
    if (counter == scale - 10) {
      tree.Draw(bpm, "after_remove.dot");
      break;
    }
    index_key.SetFromInteger(key);
    tree.Remove(index_key, transaction);
    c_keys.pop_back();
    std::vector<RID> rids;
    for (auto key2 : c_keys) {
      rids.clear();
      index_key.SetFromInteger(key2);
      tree.GetValue(index_key, &rids);
      EXPECT_EQ(rids.size(), 1);
      int64_t value = key2 & 0xFFFFFFFF;
      EXPECT_EQ(rids[0].GetSlotNum(), value);
    }
    counter++;
  }
  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");

}
}  // namespace bustub

//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdarg>
#include <cstdlib>
#include <functional>
#include <list>
#include <memory>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size) : bucket_size_(bucket_size) {
  for (int i = 0; i < num_buckets_; i++) {
    dir_.emplace_back(new Bucket(bucket_size_));
  }
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  auto index = IndexOf(key);
  std::lock_guard<std::mutex> lock(this->latch_);
  return dir_[index]->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  auto index = IndexOf(key);
  std::lock_guard<std::mutex> lock(this->latch_);
  return dir_[index]->Remove(key);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::ExpandDirectory() -> void {
  size_t size = dir_.size();
  for (size_t i = 0; i < size; i++) {
    dir_.emplace_back(dir_[i]);
  }
  // num_buckets_ *= 2;
  global_depth_++;
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  auto index = IndexOf(key);
  std::lock_guard<std::mutex> lock(this->latch_);
  std::shared_ptr<Bucket> cur_bucket = dir_[index];

  while (!cur_bucket->Insert(key, value)) {
    if (cur_bucket->GetDepth() == global_depth_) {
      ExpandDirectory();
    }
    RedistributeBucket(cur_bucket);
    index = IndexOf(key);
    cur_bucket = dir_[index];
  }
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::RedistributeBucket(std::shared_ptr<Bucket> bucket) {
  auto bucket0 = std::shared_ptr<Bucket>(new Bucket(bucket_size_, bucket->GetDepth() + 1));
  auto bucket1 = std::shared_ptr<Bucket>(new Bucket(bucket_size_, bucket->GetDepth() + 1));
  num_buckets_++;
  int mask = (1 << bucket->GetDepth());
  for (auto [k, v] : bucket->GetItems()) {
    if (std::hash<K>()(k) & mask) {
      bucket1->Insert(k, v);
    } else {
      bucket0->Insert(k, v);
    }
  }
  for (size_t i = 0; i < dir_.size(); i++) {
    if (dir_[i] == bucket) {
      dir_[i] = static_cast<bool>(i & mask) ? bucket1 : bucket0;
    }
  }
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for (const auto &[k, v] : list_) {
    if (k == key) {
      value = v;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  for (auto it = list_.begin(); it != list_.end(); ++it) {
    if (it->first == key) {
      list_.erase(it);
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  if (IsFull()) {
    return false;
  }
  for (auto &[k, v] : list_) {
    if (k == key) {
      v = value;
      return true;
    }
  }
  list_.emplace_back(key, value);
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub

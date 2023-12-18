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
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  dir_.emplace_back(std::make_shared<Bucket>(bucket_size_));
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
auto ExtendibleHashTable<K, V>::GetIndexBucket(int dir_index) const -> std::shared_ptr<Bucket> {
  return dir_[dir_index];
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
  std::scoped_lock<std::mutex> lock(latch_);
  auto bucket = GetIndexBucket(IndexOf(key));
  return bucket->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  auto bucket = GetIndexBucket(IndexOf(key));
  return bucket->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch_);
  // 先获取对应的bucket
  auto bucket = GetIndexBucket(IndexOf(key));
  // 一直不停的循环插入，直到能插入成功为止
  while (!bucket->Insert(key, value)) {
    // 插入失败
    // 拿出原来在bucket的项，需要重新分配
    std::list<std::pair<K, V>> list = bucket->GetItems();
    bucket->GetItems().clear();
    if (GetLocalDepthInternal(IndexOf(key)) == GetGlobalDepthInternal()) {
      // 当LocalDepth==GlobalDepth时，需要扩展dir
      // 增长direcotry和global_depth的大小
      std::vector<std::shared_ptr<Bucket>> dir(dir_);
      global_depth_++;
      unsigned int n = dir_.size();
      for (unsigned int i = 0; i < n; i++) {
        std::shared_ptr<Bucket> sp = dir_[i];
        dir_.emplace_back(sp);
      }
    }
    unsigned first = std::hash<K>()(key) & ((1 << bucket->GetDepth()) - 1);
    bucket->IncrementDepth();

    bool build = false;
    std::shared_ptr<Bucket> new_bucket;
    for (unsigned int i = first + 1; i < dir_.size(); i++) {
      if ((i & ((1 << (bucket->GetDepth() - 1)) - 1)) == first) {
        unsigned second = i & ((1 << bucket->GetDepth()) - 1);
        if (first != second) {
          auto share_bucket = GetIndexBucket(i);
          share_bucket.reset();
          if (!build) {
            new_bucket = std::make_shared<Bucket>(bucket_size_, bucket->GetDepth());
            build = true;
            num_buckets_++;
          }
          dir_[i] = new_bucket;
          // std::cout << i << " " << share_bucket << std::endl;
        }
      }
    }
    for (auto &element : list) {
      GetIndexBucket(IndexOf(element.first))->Insert(element.first, element.second);
    }
    // 重新获取bucket
    bucket = GetIndexBucket(IndexOf(key));
  }
  /*std::cout << key << " " << value << " " << std::hex << std::hash<K>()(key) << std::endl;
  for(unsigned int i=0; i<dir_.size(); i++) {
    std::list<std::pair<K, V>> list = GetIndexBucket(i)->GetItems();
    printf("---dir_index(%d)--- \n", i);
    for(auto & it : list){
      std::cout << it.first << " " << it.second << std::endl;
    }
    printf("--------------------\n");
  }*/
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::RedistributeBucket(std::shared_ptr<Bucket> bucket) -> void {
  // bucket = std::move(std::make_shared<Bucket>(bucket_size_, bucket->GetDepth));
}
//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for (auto iter = list_.begin(); iter != list_.end(); iter++) {
    if (iter->first == key) {
      value = iter->second;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  for (auto iter = list_.begin(); iter != list_.end(); iter++) {
    if (iter->first == key) {
      list_.erase(iter);
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  if (IsFull()) {
    // bucket满了
    return false;
  }

  for (auto iter = list_.begin(); iter != list_.end(); iter++) {
    if (iter->first == key) {
      // 原先存在key,则更新value
      iter->second = value;
      return true;
    }
  }
  // 没有则插入
  list_.emplace_back(std::make_pair(key, value));
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
// template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
// template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub

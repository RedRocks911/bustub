//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  dummy_history_ = new Node();
  dummy_buffer_ = new Node();
  dummy_history_->prev_ = dummy_buffer_;
  dummy_history_->next_ = dummy_buffer_;
  dummy_buffer_->prev_ = dummy_history_;
  dummy_buffer_->next_ = dummy_history_;
}

LRUKReplacer::~LRUKReplacer() {
  for (auto &entrie : entries_) {
    delete entrie.second;
  }
  delete dummy_history_;
  delete dummy_buffer_;
}
auto LRUKReplacer::EvictInternal(frame_id_t *frame_id) -> bool {
  if (frame_id == nullptr) {
    return false;
  }
  if (Size() == 0) {
    return false;
  }
  Node *node;
  node = dummy_history_->next_;
  while (node != nullptr && node != dummy_history_) {
    LOG_INFO("(key)%d, (value)%ld#", node->key_, node->value_);
    if (node == dummy_buffer_) {
      LOG_INFO("tttttttttttttt#");
    }
    node = node->next_;
  }
  LOG_INFO("---------------------#");
  if (curr_history_size_ != 0) {
    node = dummy_buffer_->prev_;
    curr_history_size_--;
  } else {
    node = dummy_history_->prev_;
    curr_buffer_size_--;
  }

  RemoveFromPool(node);
  node->evictable_ = false;
  node->value_ = 0;
  *frame_id = node->key_;
  return true;
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  return EvictInternal(frame_id);
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  LOG_INFO("RecordAccess (key)%d#", frame_id);
  auto it = entries_.find(frame_id);
  Node *node;
  if (it == entries_.end()) {
    entries_[frame_id] = node = new Node(frame_id, 1, false);
    node->value_ = 1;
  } else {
    node = it->second;
    node->value_++;
    if (node->evictable_ && node->value_ >= k_) {
      if (node->value_ == k_) {
        curr_history_size_--;
      } else {
        curr_buffer_size_--;
      }
      RemoveFromPool(node);
      PushFrontBufferPool(node);
      curr_buffer_size_++;
    }
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  LOG_INFO("SetEvictable (key)%d, (evictable)%d#", frame_id, set_evictable);
  auto it = entries_.find(frame_id);
  if (it == entries_.end()) {
    return;
  }
  auto node = it->second;
  // printf("111111 %d-%d\n ",frame_id, node->evictable_);
  if (node->evictable_ && !set_evictable) {
    if (node->value_ >= k_) {
      curr_buffer_size_--;
    } else {
      curr_history_size_--;
    }
    RemoveFromPool(node);
    node->evictable_ = false;
    node->value_ = 0;
  } else if (!node->evictable_ && set_evictable) {
    while (Size() >= replacer_size_) {
      frame_id_t frame_evict_id;
      EvictInternal(&frame_evict_id);
    }
    node->evictable_ = true;
    if (node->value_ >= k_) {
      PushFrontBufferPool(node);
      curr_buffer_size_++;
      // printf("curr_buffer_size_ %ld ", curr_buffer_size_);
    } else {
      PushFrontHistoryPool(node);
      curr_history_size_++;
      // printf("curr_history_size_ %ld ", curr_history_size_);
    }
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  auto it = entries_.find(frame_id);
  if (it == entries_.end()) {
    return;
  }
  auto node = it->second;
  if (node->evictable_) {
    if (node->value_ >= k_) {
      curr_buffer_size_--;
    } else {
      curr_history_size_--;
    }
    RemoveFromPool(node);
    node->evictable_ = false;
    node->value_ = 0;
  }
  delete it->second;
  entries_.erase(frame_id);
}

auto LRUKReplacer::Size() -> size_t { return curr_history_size_ + curr_buffer_size_; }

}  // namespace bustub

//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.h
//
// Identification: src/include/buffer/lru_k_replacer.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <limits>
#include <list>
#include <mutex>  // NOLINT
#include <unordered_map>
#include <vector>

#include "common/config.h"
#include "common/logger.h"
#include "common/macros.h"

namespace bustub {
class Node {
 public:
  frame_id_t key_{0};
  size_t value_{0};
  bool evictable_{false};
  Node *prev_{nullptr};
  Node *next_{nullptr};

  explicit Node(frame_id_t key = 0, size_t value = 0, bool evictable = false) {
    key_ = key;
    value_ = value;
    evictable_ = evictable;
  }
};
/**
 * LRUKReplacer implements the LRU-k replacement policy.
 *
 * The LRU-k algorithm evicts a frame whose backward k-distance is maximum
 * of all frames. Backward k-distance is computed as the difference in time between
 * current timestamp and the timestamp of kth previous access.
 *
 * A frame with less than k historical references is given
 * +inf as its backward k-distance. When multiple frames have +inf backward k-distance,
 * classical LRU algorithm is used to choose victim.
 */
class LRUKReplacer {
 public:
  /**
   *
   * TODO(P1): Add implementation
   *
   * @brief a new LRUKReplacer.
   * @param num_frames the maximum number of frames the LRUReplacer will be required to store
   */
  explicit LRUKReplacer(size_t num_frames, size_t k);

  DISALLOW_COPY_AND_MOVE(LRUKReplacer);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Destroys the LRUReplacer.
   */
  ~LRUKReplacer();

  /**
   * TODO(P1): Add implementation
   *
   * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
   * that are marked as 'evictable' are candidates for eviction.
   *
   * A frame with less than k historical references is given +inf as its backward k-distance.
   * If multiple frames have inf backward k-distance, then evict the frame with the earliest
   * timestamp overall.
   *
   * Successful eviction of a frame should decrement the size of replacer and remove the frame's
   * access history.
   *
   * @param[out] frame_id id of frame that is evicted.
   * @return true if a frame is evicted successfully, false if no frames can be evicted.
   */
  auto Evict(frame_id_t *frame_id) -> bool;

  /**
   * TODO(P1): Add implementation
   *
   * @brief Record the event that the given frame id is accessed at current timestamp.
   * Create a new entry for access history if frame id has not been seen before.
   *
   * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
   * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
   *
   * @param frame_id id of frame that received a new access.
   */
  void RecordAccess(frame_id_t frame_id);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Toggle whether a frame is evictable or non-evictable. This function also
   * controls replacer's size. Note that size is equal to number of evictable entries.
   *
   * If a frame was previously evictable and is to be set to non-evictable, then size should
   * decrement. If a frame was previously non-evictable and is to be set to evictable,
   * then size should increment.
   *
   * If frame id is invalid, throw an exception or abort the process.
   *
   * For other scenarios, this function should terminate without modifying anything.
   *
   * @param frame_id id of frame whose 'evictable' status will be modified
   * @param set_evictable whether the given frame is evictable or not
   */
  void SetEvictable(frame_id_t frame_id, bool set_evictable);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Remove an evictable frame from replacer, along with its access history.
   * This function should also decrement replacer's size if removal is successful.
   *
   * Note that this is different from evicting a frame, which always remove the frame
   * with largest backward k-distance. This function removes specified frame id,
   * no matter what its backward k-distance is.
   *
   * If Remove is called on a non-evictable frame, throw an exception or abort the
   * process.
   *
   * If specified frame is not found, directly return from this function.
   *
   * @param frame_id id of frame to be removed
   */
  void Remove(frame_id_t frame_id);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Return replacer's size, which tracks the number of evictable frames.
   *
   * @return size_t
   */
  auto Size() -> size_t;

 private:
  // TODO(student): implement me! You can replace these member variables as you like.
  // Remove maybe_unused if you start using them.
  // [[maybe_unused]] size_t current_timestamp_{0};
  size_t curr_history_size_{0};  // the number of history pool evictable frames
  size_t curr_buffer_size_{0};   // the number of buffer pool evictable frames
  size_t replacer_size_;         // the capacity of evictable frames in LRUReplacer
  size_t k_;
  Node *dummy_history_;
  Node *dummy_buffer_;
  std::unordered_map<frame_id_t, Node *> entries_;
  std::mutex latch_;
  auto EvictInternal(frame_id_t *frame_id) -> bool;

  void RemoveFromPool(Node *x) {
    if (x->prev_ == nullptr || x->next_ == nullptr) {
      return;
    }
    LOG_INFO("RemoveFromPool (key)%d, (value)%ld#", x->key_, x->value_);
    x->prev_->next_ = x->next_;
    x->next_->prev_ = x->prev_;
    x->prev_ = nullptr;
    x->next_ = nullptr;
  }

  void PushFrontHistoryPool(Node *x) {
    LOG_INFO("PushFrontHistoryPool (key)%d, (value)%ld#", x->key_, x->value_);
    dummy_history_->next_->prev_ = x;
    x->next_ = dummy_history_->next_;
    dummy_history_->next_ = x;
    x->prev_ = dummy_history_;
  }

  void PushFrontBufferPool(Node *x) {
    LOG_INFO("PushFrontBufferPool (key)%d, (value)%ld#", x->key_, x->value_);
    dummy_buffer_->next_->prev_ = x;
    x->next_ = dummy_buffer_->next_;
    dummy_buffer_->next_ = x;
    x->prev_ = dummy_buffer_;
  }

  /*Node *get_node(frame_id_t key) {
    auto it = entries_.find(key);
    if (it == entries_.end()) {
      return nullptr;
    }
    auto node = it->second;
    if (node->evictable_) {
      node->value_++;
      if (node->value_ >= k_) {
        RemoveFromLink(node);
        push_after_node(node, dummy_buffer_);
      }
    }
  }*/
};

}  // namespace bustub

//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// p0_trie.h
//
// Identification: src/include/primer/p0_trie.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <bits/stdc++.h>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rwlatch.h"

namespace bustub {

/**
 * TrieNode is a generic container for any node in Trie.
 */
class TrieNode {
 public:
  /**
   * TODO(P0): Add implementation
   *
   * @brief Construct a new Trie Node object with the given key char.
   * is_end_ flag should be initialized to false in this constructor.
   *
   * @param key_char Key character of this trie node
   */
  explicit TrieNode(char key_char) : key_char_(key_char) {}

  /**
   * TODO(P0): Add implementation
   *
   * @brief Move constructor for trie node object. The unique pointers stored
   * in children_ should be moved from other_trie_node to new trie node.
   *
   * @param other_trie_node Old trie node.
   */
  TrieNode(TrieNode &&other_trie_node) noexcept
      : key_char_(other_trie_node.key_char_),
        is_end_(other_trie_node.is_end_),
        children_(std::move(other_trie_node.children_)) {}

  /**
   * @brief Destroy the TrieNode object.
   */
  virtual ~TrieNode() = default;

  /**
   * TODO(P0): Add implementation
   *
   * @brief Whether this trie node has a child node with specified key char.
   *
   * @param key_char Key char of child node.
   * @return True if this trie node has a child with given key, false otherwise.
   */
  bool HasChild(char key_char) const { return children_.find(key_char) != children_.end(); }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Whether this trie node has any children at all. This is useful
   * when implementing 'Remove' functionality.
   *
   * @return True if this trie node has any child node, false if it has no child node.
   */
  bool HasChildren() const { return !children_.empty(); }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Whether this trie node is the ending character of a key string.
   *
   * @return True if is_end_ flag is true, false if is_end_ is false.
   */
  bool IsEndNode() const { return is_end_; }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Return key char of this trie node.
   *
   * @return key_char_ of this trie node.
   */
  char GetKeyChar() const { return key_char_; }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Insert a child node for this trie node into children_ map, given the key char and
   * unique_ptr of the child node. If specified key_char already exists in children_,
   * return nullptr. If parameter `child`'s key char is different than parameter
   * `key_char`, return nullptr.
   *
   * Note that parameter `child` is rvalue and should be moved when it is
   * inserted into children_map.
   *
   * The return value is a pointer to unique_ptr because pointer to unique_ptr can access the
   * underlying data without taking ownership of the unique_ptr. Further, we can set the return
   * value to nullptr when error occurs.
   *
   * @param key Key of child node
   * @param child Unique pointer created for the child node. This should be added to children_ map.
   * @return Pointer to unique_ptr of the inserted child node. If insertion fails, return nullptr.
   */
  std::unique_ptr<TrieNode> *InsertChildNode(char key_char, std::unique_ptr<TrieNode> &&child) {
    if (key_char == child->GetKeyChar() && children_.find(key_char) == children_.end()) {
      children_[key_char] = std::move(child);
      return &children_[key_char];
    }
    return nullptr;
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Get the child node given its key char. If child node for given key char does
   * not exist, return nullptr.
   *
   * @param key Key of child node
   * @return Pointer to unique_ptr of the child node, nullptr if child
   *         node does not exist.
   */
  std::unique_ptr<TrieNode> *GetChildNode(char key_char) {
    /*if (!HasChild(key_char)) {
      return nullptr;
    }
    return &children_[key_char];*/
    if (children_.find(key_char) != children_.end()) {
      return &children_[key_char];
    }
    return nullptr;
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Remove child node from children_ map.
   * If key_char does not exist in children_, return immediately.
   *
   * @param key_char Key char of child node to be removed
   */
  void RemoveChildNode(char key_char) {
    if (children_.find(key_char) != children_.end()) {
      // children_[key_char].reset();
      children_.erase(key_char);
    }
  }
  /*void RemoveChildNode(std::unique_ptr<TrieNode> node) {
    if(!node) return;
    for(auto iter=children_.begin(); iter!=children_.end(); iter++){
      RemoveChildNode(iter->second());
    }
  }*/
  /**
   * TODO(P0): Add implementation
   *
   * @brief Set the is_end_ flag to true or false.
   *
   * @param is_end Whether this trie node is ending char of a key string
   */
  void SetEndNode(bool is_end) { is_end_ = is_end; }

 protected:
  /** Key character of this trie node */
  char key_char_;
  /** whether this node marks the end of a key */
  bool is_end_{false};
  /** A map of all child nodes of this trie node, which can be accessed by each
   * child node's key char. */
  std::unordered_map<char, std::unique_ptr<TrieNode>> children_;
};

/**
 * TrieNodeWithValue is a node that marks the ending of a key, and it can
 * hold a value of any type T.
 */
template <typename T>
class TrieNodeWithValue : public TrieNode {
 private:
  /* Value held by this trie node. */
  T value_;

 public:
  /**
   * TODO(P0): Add implementation
   *
   * @brief Construct a new TrieNodeWithValue object from a TrieNode object and specify its value.
   * This is used when a non-terminal TrieNode is converted to terminal TrieNodeWithValue.
   *
   * The children_ map of TrieNode should be moved to the new TrieNodeWithValue object.
   * Since it contains unique pointers, the first parameter is a rvalue reference.
   *
   * You should:
   * 1) invoke TrieNode's move constructor to move data from TrieNode to
   * TrieNodeWithValue.
   * 2) set value_ member variable of this node to parameter `value`.
   * 3) set is_end_ to true
   *
   * @param trieNode TrieNode whose data is to be moved to TrieNodeWithValue
   * @param value
   */
  TrieNodeWithValue(TrieNode &&trieNode, T value) : TrieNode(std::move(trieNode)), value_(value) { is_end_ = true; }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Construct a new TrieNodeWithValue. This is used when a new terminal node is constructed.
   *
   * You should:
   * 1) Invoke the constructor for TrieNode with the given key_char.
   * 2) Set value_ for this node.
   * 3) set is_end_ to true.
   *
   * @param key_char Key char of this node
   * @param value Value of this node
   */
  TrieNodeWithValue(char key_char, T value) : TrieNode(key_char), value_(value) { is_end_ = true; }

  /**
   * @brief Destroy the Trie Node With Value object
   */
  ~TrieNodeWithValue() override = default;

  /**
   * @brief Get the stored value_.
   *
   * @return Value of type T stored in this node
   */
  T GetValue() const { return value_; }
};

/**
 * Trie is a concurrent key-value store. Each key is a string and its corresponding
 * value can be any type.
 */
class Trie {
 private:
  /* Root node of the trie */
  std::unique_ptr<TrieNode> root_;
  /* Read-write lock for the trie */
  ReaderWriterLatch latch_;

 public:
  /**
   * TODO(P0): Add implementation
   *
   * @brief Construct a new Trie object. Initialize the root node with '\0'
   * character.
   */
  Trie() { root_ = std::make_unique<TrieNode>('\0'); }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Insert key-value pair into the trie.
   *
   * If the key is an empty string, return false immediately.
   *
   * If the key already exists, return false. Duplicated keys are not allowed and
   * you should never overwrite value of an existing key.
   *
   * When you reach the ending character of a key:
   * 1. If TrieNode with this ending character does not exist, create new TrieNodeWithValue
   * and add it to parent node's children_ map.
   * 2. If the terminal node is a TrieNode, then convert it into TrieNodeWithValue by
   * invoking the appropriate constructor.
   * 3. If it is already a TrieNodeWithValue,
   * then insertion fails and returns false. Do not overwrite existing data with new data.
   *
   * You can quickly check whether a TrieNode pointer holds TrieNode or TrieNodeWithValue
   * by checking the is_end_ flag. If is_end_ == false, then it points to TrieNode. If
   * is_end_ == true, it points to TrieNodeWithValue.
   *
   * @param key Key used to traverse the trie and find the correct node
   * @param value Value to be inserted
   * @return True if insertion succeeds, false if the key already exists
   */
  template <typename T>
  bool Insert(const std::string &key, T value) {
    // std::cout << "Insert trie = " << this << ", key = " << key << ", value = " << value << std::endl;
    bool ret = false;
    if (key.empty()) {
      return false;
    }
    latch_.WLock();
    std::unique_ptr<TrieNode> *node = &root_;
    int i = 0;
    int n = key.size();
    for (i = 0; i < n - 1; i++) {
      if (node->get()->GetChildNode(key[i]) == nullptr) {
        std::unique_ptr<TrieNode> tmp = std::make_unique<TrieNode>(key[i]);
        node->get()->InsertChildNode(key[i], std::move(tmp));
      }
      // printf("%d %c\n", i, key[i]);
      node = node->get()->GetChildNode(key[i]);
    }
    char ch = key[key.length() - 1];
    std::unique_ptr<TrieNode> *terminal_node = node->get()->GetChildNode(ch);
    // 最后一个字符
    if (terminal_node == nullptr) {
      // 如果没有子节点则创建终末结节点插入
      // LOG_INFO("没有子节点则创建终末结节点插入#");
      std::unique_ptr<TrieNode> tmp = std::make_unique<TrieNodeWithValue<T>>(ch, value);
      node->get()->InsertChildNode(ch, std::move(tmp));
      node->get()->SetEndNode(false);
      ret = true;
    } else if (!terminal_node->get()->IsEndNode()) {
      // 如果有非终末节点则强制改为终节点
      // LOG_INFO("非终末节点则强制改为终末节点#");
      auto *tmp = new TrieNodeWithValue<T>(std::move(*(terminal_node->get())), value);
      terminal_node->reset(tmp);
      ret = true;
    } else {
      // 如果有终末节点，不作操作，返回错误
      // LOG_INFO("如果有终末节点，不作操作，返回错误#");
      ret = false;
    }
    latch_.WUnlock();
    return ret;
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Remove key value pair from the trie.
   * This function should also remove nodes that are no longer part of another
   * key. If key is empty or not found, return false.
   *
   * You should:
   * 1) Find the terminal node for the given key.
   * 2) If this terminal node does not have any children, remove it from its
   * parent's children_ map.
   * 3) Recursively remove nodes that have no children and are not terminal node
   * of another key.
   *
   * @param key Key used to traverse the trie and find the correct node
   * @return True if the key exists and is removed, false otherwise
   */
  bool Remove(const std::string &key) {
    // LOG_INFO("Remove key(%s)#", key.c_str());
    if (key.empty()) {
      return false;
    }
    bool ret = false;
    std::function<bool(std::unique_ptr<TrieNode> *, std::string &)> dfs = [&](std::unique_ptr<TrieNode> *node,
                                                                              std::string &key) -> bool {
      if (node == nullptr) {
        return false;
      }
      if (key.size() == 1) {
        if (node->get()->IsEndNode()) {
          // LOG_INFO("node set not end node (key)%c#", node->get()->GetKeyChar());
          node->get()->SetEndNode(false);
        }
        return true;
      }
      // node != nullptr && key.size()!=0,继续向下递归
      std::string subkey = key.substr(1, key.size() - 1);
      // printf("key[0] = %c\n", key[0]);
      ret = dfs(node->get()->GetChildNode(key[1]), subkey);

      if (!ret) {
        return false;
      }

      auto subnode = node->get()->GetChildNode(key[1]);
      if (subnode != nullptr && !subnode->get()->HasChildren() && !subnode->get()->IsEndNode()) {
        // 当找到了并且子节点没有子节点，那就删除子节点
        // LOG_INFO("remove chiled node\n");
        node->get()->RemoveChildNode(key[1]);
      }
      return true;
    };
    latch_.WLock();
    auto node = &root_;
    ret = dfs(node->get()->GetChildNode(key[0]), const_cast<std::string &>(key));
    latch_.WUnlock();
    // printf("-----------------\n");
    return ret;
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Get the corresponding value of type T given its key.
   * If key is empty, set success to false.
   * If key does not exist in trie, set success to false.
   * If the given type T is not the same as the value type stored in TrieNodeWithValue
   * (ie. GetValue<int> is called but terminal node holds std::string),
   * set success to false.
   *
   * To check whether the two types are the same, dynamic_cast
   * the terminal TrieNode to TrieNodeWithValue<T>. If the casted result
   * is not nullptr, then type T is the correct type.
   *
   * @param key Key used to traverse the trie and find the correct node
   * @param success Whether GetValue is successful or not
   * @return Value of type T if type matches
   */
  template <typename T>
  T GetValue(const std::string &key, bool *success) {
    // LOG_INFO("GetValue key(%s)#", key.c_str());
    if (key.empty()) {
      *success = false;
      return {};
    }
    latch_.RLock();
    std::unique_ptr<TrieNode> *node = &root_;
    for (const auto &c : key) {
      // LOG_INFO("ccccc(%c)#", c);
      node = node->get()->GetChildNode(c);
      if (node == nullptr) {
        // LOG_INFO("333333#");
        *success = false;
        latch_.RUnlock();
        return {};
      }
    }

    T res;
    auto flag_node = dynamic_cast<TrieNodeWithValue<T> *>(node->get());
    if (flag_node == nullptr) {
      // LOG_INFO("111111#");
      *success = false;
      latch_.RUnlock();
      return {};
    }
    // LOG_INFO("222222#");
    // LOG_INFO("flag_node(%p)#",flag_node);
    *success = true;
    // LOG_INFO("333333#");
    // LOG_INFO("flag_node(%p)#",flag_node);
    res = flag_node->GetValue();
    // LOG_INFO("444444#");
    latch_.RUnlock();
    return res;
  }
};
}  // namespace bustub

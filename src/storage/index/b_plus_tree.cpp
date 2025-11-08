#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPage(const KeyType &key) const -> LeafPage * {
  page_id_t page_id = root_page_id_;
  auto page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(page_id)->GetData());
  while (!page->IsLeafPage()) {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    int index = internal->Find(key, comparator_);
    page_id = internal->ValueAt(index);
    page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(page_id)->GetData());
    buffer_pool_manager_->UnpinPage(internal->GetPageId(), false);
  }
  return reinterpret_cast<LeafPage *>(page);
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  auto leaf = FindLeafPage(key);
  int index = leaf->Find(key, comparator_);
  if (index == INVALID_INDEX_ID) {
    return false;
  }
  result->emplace_back(leaf->ValueAt(index));
  buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitLeafPage(LeafPage *page) -> page_id_t {
  page_id_t page_id_;
  int size = page->GetSize();
  // 当前pagesize超过最大值,分裂成两个page,新建page
  auto *newPage = reinterpret_cast<LeafPage *>(buffer_pool_manager_->NewPage(&page_id_)->GetData());
  newPage->Init(page_id_, page->GetParentPageId(), leaf_max_size_);
  int splitIndex = size/2;
  // 把数据移到新page里
  for (int i = splitIndex; i<size; i++) {
    newPage->SetKeyValueAt(i-splitIndex, page->KeyAt(i), page->ValueAt(i));
  }
  // 设置page长度
  newPage->SetSize(size-splitIndex);
  page->SetSize(splitIndex);

  page_id_t parent_pid;
  if (page->GetPageId() == root_page_id_) {
    // 如果是rootpage,则新建rootpage
    auto *newRootPage = reinterpret_cast<InternalPage *>(buffer_pool_manager_->NewPage(&root_page_id_)->GetData());
    newRootPage->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);
    newRootPage->SetKeyValueAt(0, KeyType(), page->GetPageId());
    newRootPage->SetKeyValueAt(1, newPage->KeyAt(0), newPage->GetPageId());
    newRootPage->IncreaseSize(2);
    UpdateRootPageId();
    page->SetParentPageId(root_page_id_);
    parent_pid = root_page_id_;
  }else {
    parent_pid = page->GetParentPageId();
    auto parentPage = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent_pid)->GetData());
    parentPage->InsertDataToPage(newPage->KeyAt(0), newPage->GetPageId(), comparator_);
  } 

  newPage->SetParentPageId(parent_pid);
  newPage->SetNextPageId(page->GetNextPageId());
  page->SetNextPageId(newPage->GetPageId());
  buffer_pool_manager_->UnpinPage(page_id_, true);
  buffer_pool_manager_->UnpinPage(parent_pid, true);

  return parent_pid;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertLeafPage(const KeyType &key, const ValueType &value, Transaction *transaction) -> void {
  auto *leaf = FindLeafPage(key);
  int index = leaf->Find(key, comparator_);
  if (index != INVALID_INDEX_ID) {
    // 原先有数据,覆盖原有数据
    leaf->SetKeyValueAt(index, key, value);
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
    return;
  }
  leaf->InsertDataToPage(key, value, comparator_);
  if (leaf->GetSize() <= leaf->GetMaxSize()) {
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
    return;
  }
  page_id_t current_page_id  = SplitLeafPage(leaf);
  
  auto current_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(current_page_id)->GetData());
  while (current_page != nullptr && current_page->GetSize() > current_page->GetMaxSize()) {
    page_id_t new_page_id_;
    int size = current_page->GetSize();
    // 当前pagesize超过最大值,分裂成两个page,新建page
    auto *newPage = reinterpret_cast<InternalPage *>(buffer_pool_manager_->NewPage(&new_page_id_)->GetData());
    newPage->Init(new_page_id_, current_page->GetParentPageId(), internal_max_size_);
    int splitIndex = size/2;
    // 把数据移到新page里
    for (int i = splitIndex; i<size; i++) {
      newPage->SetKeyValueAt(i-splitIndex, current_page->KeyAt(i), current_page->ValueAt(i));
    }
    // 设置page长度
    newPage->SetSize(size-splitIndex);
    current_page->SetSize(splitIndex);
    if (current_page->GetPageId() == root_page_id_) {
      // 如果是rootpage,则新建rootpage
      auto *newRootPage = reinterpret_cast<InternalPage *>(buffer_pool_manager_->NewPage(&root_page_id_)->GetData());
      newRootPage->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);
      newRootPage->SetKeyValueAt(0, KeyType(), current_page->GetPageId());
      newRootPage->SetKeyValueAt(1, newPage->KeyAt(0), newPage->GetPageId());
      newRootPage->IncreaseSize(2);
      newPage->SetKeyValueAt(0, KeyType(), newPage->ValueAt(0));
      UpdateRootPageId();
      current_page->SetParentPageId(root_page_id_);
      newPage->SetParentPageId(root_page_id_);
    } else {
      auto parentPage = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(current_page->GetParentPageId())->GetData());
      parentPage->InsertDataToPage(newPage->KeyAt(0), newPage->GetPageId(), comparator_);
      newPage->SetKeyValueAt(0, KeyType(), newPage->ValueAt(0));
      newPage->SetParentPageId(current_page->GetParentPageId());
      buffer_pool_manager_->UnpinPage(parentPage->GetPageId(), true);
    }
    page_id_t parent_pid = current_page->GetParentPageId();
    buffer_pool_manager_->UnpinPage(newPage->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(current_page->GetPageId(), true);
    if (parent_pid != INVALID_PAGE_ID) {
      current_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent_pid)->GetData());
    } else {
      current_page = nullptr;
    }
  }
  buffer_pool_manager_->UnpinPage(current_page->GetPageId(), true);
  return;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  if (IsEmpty()) {
    auto *root_page = buffer_pool_manager_->NewPage(&root_page_id_);
    auto *leaf_page = reinterpret_cast<LeafPage *>(root_page->GetData());
    leaf_page->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
    UpdateRootPageId(true);
    leaf_page->InsertDataToPage(key, value, comparator_);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    return true;
  }
  InsertLeafPage(key, value, transaction);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::isRedistribute(N *node) {
  return node->GetSize() > node->GetMinSize();
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::isCoalesce(N *lNode, N *rNode) {
  return lNode->GetSize() + rNode->GetSize() <= rNode->GetMaxSize();
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *lNode, N *rNode, InternalPage *parent, int index, Transaction *transaction) {
  if (lNode->GetSize() < lNode->GetMinSize()) {
    // 左节点向右节点借
    if (lNode->IsLeafPage()) {
      // 将parent的index序列的key值设为rNode的1序列key值
      parent->SetKeyValueAt(index,rNode->KeyAt(1), parent->ValueAt(index));
      // 将rNode的0序列移到lNode的GetSize序列
      lNode->SetKeyValueAt(lNode->GetSize(), rNode->KeyAt(0), rNode->ValueAt(0));
      // rNode从序列1开始全体左移一个位置
      for (int i = 1; i < rNode->GetSize(); i++) {
        rNode->SetKeyValueAt(i-1, rNode->KeyAt(i), rNode->ValueAt(i));
      }
    } else {
      // 将rNode的0序列key设为parent的index序列的key值
      rNode->SetKeyValueAt(0, parent->KeyAt(index), rNode->ValueAt(0));
      // 将parent的index序列的key值设为rNode的1序列key值
      parent->SetKeyValueAt(index,rNode->KeyAt(1), parent->ValueAt(index));
      // 将rNode的1序列key设为空
      rNode->SetKeyValueAt(1, KeyType(), rNode->ValueAt(1));
      // 将rNode的0序列移到lNode的GetSize序列
      lNode->SetKeyValueAt(lNode->GetSize(), rNode->KeyAt(0), rNode->ValueAt(0));
      // rNode从序列1开始全体左移一个位置
      for (int i = 1; i < rNode->GetSize(); i++) {
        rNode->SetKeyValueAt(i-1, rNode->KeyAt(i), rNode->ValueAt(i));
      }
    }
  } else {
    // 右节点向左节点借
    if (rNode->IsLeafPage()) {
      // rNode从序列0开始全体右移一个位置
      for (int i = 0; i < rNode->GetSize(); i++) {
        rNode->SetKeyValueAt(i+1, rNode->KeyAt(i), rNode->ValueAt(i));
      }
      // 将lNode的GetSize-1序列移到rNode的0序列
      int moveIndex = lNode->GetSize()-1;
      rNode->SetKeyValueAt(0, lNode->KeyAt(moveIndex), lNode->ValueAt(moveIndex));
      // 将parent的index序列的key值设为rNode的1序列key值
      parent->SetKeyValueAt(index,rNode->KeyAt(0), parent->ValueAt(index));
    } else {
      // rNode从序列0开始全体右移一个位置
      for (int i = 0; i < rNode->GetSize(); i++) {
        rNode->SetKeyValueAt(i+1, rNode->KeyAt(i), rNode->ValueAt(i));
      }
      // 将rNode的1序列key设为parent的index序列的key值
      rNode->SetKeyValueAt(1, parent->KeyAt(index), rNode->ValueAt(1));
      // 将lNode的move序列移到rNode的0序列
      int moveIndex = lNode->GetSize()-1;
      rNode->SetKeyValueAt(0, lNode->KeyAt(moveIndex), lNode->ValueAt(moveIndex));
      // 将parent的index序列的key值设为rNode的0序列key值
      parent->SetKeyValueAt(index,rNode->KeyAt(0), parent->ValueAt(index));
      // 将rNode的0序列key设为空
      rNode->SetKeyValueAt(0, KeyType(), rNode->ValueAt(0));
    }
  }

  lNode->IncreaseSize(1);
  rNode->IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Coalesce(N *lNode, N *rNode, InternalPage *parent, int index, Transaction *transaction) {
  if (lNode->IsLeafPage()) {
    auto lLeaf = (LeafPage *)lNode;
    auto rleaf = (LeafPage *)rNode;
    lLeaf->SetNextPageId(rleaf->GetNextPageId());
  } else {
    // 将rNode的0序列key设为parent的index序列的key值
    rNode->SetKeyValueAt(0, parent->KeyAt(index), rNode->ValueAt(0));
  }
  // rNode全体搬移到lNode
  for (int i = 0; i < lNode->GetSize(); i++) {
    lNode->SetKeyValueAt(lNode->GetSize()+i, rNode->KeyAt(i), rNode->ValueAt(i));
  }
  // 删除parent的index位
  for (int i = index+1; i < parent->GetSize(); i++) {
    parent->SetKeyValueAt(i-1, parent->KeyAt(i), parent->ValueAt(i));
  }
  lNode->IncreaseSize(rNode->GetSize());
  buffer_pool_manager_->DeletePage(rNode->GetPageId());
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::SolveOverflow(N *node, Transaction *transaction) {
  page_id_t parent_pid = node->GetParentPageId();
  auto *parentPage = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent_pid)->GetData());
  int current_index = parentPage->IndexAtOfValue(node->GetPageId());
  
  int left_sib_index = current_index - 1;
  int right_sib_index = current_index + 1;
  page_id_t sibling_pid = INVALID_PAGE_ID;
  bool borrow = false;
  if (left_sib_index >= 0) {
    sibling_pid = parentPage->ValueAt(left_sib_index);
    Page *sibling_page = buffer_pool_manager_->FetchPage(sibling_pid);
    if (node->IsLeafPage()) {
      auto *sib_node = reinterpret_cast<LeafPage *>(sibling_page->GetData());
      if (isRedistribute(sib_node)) {
        Redistribute(sib_node, reinterpret_cast<LeafPage *>(node), parentPage, current_index, transaction);
        borrow = true;
      }
    } else {
      auto *sib_node = reinterpret_cast<InternalPage *>(sibling_page->GetData());
      if (isRedistribute(sib_node)) {
        Redistribute(sib_node, reinterpret_cast<InternalPage *>(node), parentPage, current_index, transaction);
        borrow = true;
      }
    }
    // sib_node借用完毕,Unpin
    buffer_pool_manager_->UnpinPage(sibling_pid, true);
  } else if (right_sib_index < parentPage->GetSize()) {
    sibling_pid = parentPage->ValueAt(right_sib_index);
    Page *sibling_page = buffer_pool_manager_->FetchPage(sibling_pid);
    if (node->IsLeafPage()) {
      auto *sib_node = reinterpret_cast<LeafPage *>(sibling_page->GetData());
      if (isRedistribute(sib_node)) {
        Redistribute(reinterpret_cast<LeafPage *>(node), sib_node, parentPage, right_sib_index, transaction);
        borrow = true;
      }
    } else {
      auto *sib_node = reinterpret_cast<InternalPage *>(sibling_page->GetData());
      if (isRedistribute(sib_node)) {
        Redistribute(reinterpret_cast<InternalPage *>(node), sib_node, parentPage, right_sib_index, transaction);
        borrow = true;
      }
    }
    // sib_node借用完毕,Unpin
    buffer_pool_manager_->UnpinPage(sibling_pid, true);
  }
  
  if (borrow == false) {
    if (left_sib_index >= 0) {
      sibling_pid = parentPage->ValueAt(left_sib_index);
      Page *sibling_page = buffer_pool_manager_->FetchPage(sibling_pid);
      if (node->IsLeafPage()) {
        auto *sib_node = reinterpret_cast<LeafPage *>(sibling_page->GetData());
        if (isCoalesce(sib_node, reinterpret_cast<LeafPage *>(node))) {
          Coalesce(sib_node, reinterpret_cast<LeafPage *>(node), parentPage, current_index, transaction);
          // 因为node数据已经全部转移给了sib_node，并且node删除了，所以sib_node作为node
          node = reinterpret_cast<N *>(sib_node);
        }
      }else {
        auto *sib_node = reinterpret_cast<InternalPage *>(sibling_page->GetData());
        if (isCoalesce(sib_node, reinterpret_cast<InternalPage *>(node))) {
          Coalesce(sib_node, reinterpret_cast<InternalPage *>(node), parentPage, current_index, transaction);
          // 因为node数据已经全部转移给了sib_node，并且node删除了，所以sib_node作为node
          node = reinterpret_cast<N *>(sib_node);
        } 
      }
    } else {
      sibling_pid = parentPage->ValueAt(right_sib_index);
      Page *sibling_page = buffer_pool_manager_->FetchPage(sibling_pid);
      if (node->IsLeafPage()) {
        auto *sib_node = reinterpret_cast<LeafPage *>(sibling_page->GetData());
        if (isCoalesce(reinterpret_cast<LeafPage *>(node), sib_node)) {
          // sib_node已经删除，所以不用Unpin
          Coalesce(reinterpret_cast<LeafPage *>(node), sib_node, parentPage, right_sib_index, transaction);
        }
      }else {
        auto *sib_node = reinterpret_cast<InternalPage *>(sibling_page->GetData());
        if (isCoalesce(reinterpret_cast<InternalPage *>(node), sib_node)) {
          // sib_node已经删除，所以不用Unpin
          Coalesce(reinterpret_cast<InternalPage *>(node), sib_node, parentPage, right_sib_index, transaction);
        }
      }
    }
  }
  if (parentPage->GetSize() == 1 && parentPage->GetPageId() == root_page_id_) {
    // 当父节点是根节点并且长度为1时,释放,并将child page设置为新的root page
    buffer_pool_manager_->DeletePage(root_page_id_);
    root_page_id_ = node->GetPageId();
    node->SetParentPageId(INVALID_PAGE_ID);
    UpdateRootPageId(false);
  }
  buffer_pool_manager_->UnpinPage(parent_pid, true);
}
/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if (IsEmpty()) {
    return;
  }
  auto *leaf = FindLeafPage(key);
  int index = leaf->Find(key, comparator_);
  if (index != INVALID_INDEX_ID) {
    for (int i = index; i<leaf->GetSize()-1; i++){
      leaf->SetKeyValueAt(i, leaf->KeyAt(i+1), leaf->ValueAt(i+1));
    }
    leaf->IncreaseSize(-1);
  }else {
    // 没找到直接返回
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
    return;
  }

  if (leaf->GetPageId() == root_page_id_) {
    if (leaf->GetSize() == 0) {
      buffer_pool_manager_->DeletePage(root_page_id_);
    }
    return;
  }

  InternalPage *node;
  if (leaf->GetSize() < leaf->GetMinSize()) {
    SolveOverflow(leaf, transaction);
    node = reinterpret_cast<InternalPage *>(leaf);
    page_id_t parent_pid = node->GetParentPageId();
    node = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent_pid)->GetData());
    while (node != nullptr && node->GetSize() < node->GetMinSize()) {
      SolveOverflow(node, transaction);
      // 这里需重新获取下ParentPageId,有可能改变
      if ((parent_pid = node->GetParentPageId()) != INVALID_PAGE_ID) {
        buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
        node = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent_pid)->GetData());
      }else {
        break;
      }
    }
  }

  buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
  return;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  auto *leaf = FindLeafPage(KeyType{});
  return IndexIterator(leaf, 0, buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  auto *leaf = FindLeafPage(key);
  int index = leaf->Find(key, comparator_);
  return IndexIterator(leaf, index, buffer_pool_manager_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  page_id_t page_id = root_page_id_;
  auto page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(page_id)->GetData());
  while (!page->IsLeafPage()) {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    page_id = internal->ValueAt(internal->GetSize() - 1);
    page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(page_id)->GetData());
    buffer_pool_manager_->UnpinPage(internal->GetPageId(), false);
  }
  auto *leaf = reinterpret_cast<LeafPage *>(page);
  return IndexIterator(leaf, leaf->GetSize(), buffer_pool_manager_);
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub

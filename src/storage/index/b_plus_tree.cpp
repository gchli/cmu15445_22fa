#include <cassert>
#include <iostream>
#include <string>

#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "concurrency/transaction.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_page.h"
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
  /* 1. Empty tree, return false. */
  LockTreeRoot(OpType::FIND, transaction);
  if (IsEmpty()) {
    UnlatchAllPages(transaction, OpType::INSERT, false);
    return false;
  }


  /* 2. Find the target leaf page may contain the key. */
  auto leaf_page = FindLeafPage(key, OpType::FIND, transaction);

  /* 3. Look up the key in the leaf page. */
  result->resize(1);
  bool found = leaf_page->Find(key, (*result)[0], comparator_);
  if (transaction == nullptr) {
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
  } else {
    UnlatchAllPages(transaction, OpType::FIND, false);
  }
  return found;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitLeafPage(LeafPage *leaf_page, BufferPoolManager *buffer_pool_manager,
                                   Transaction *transaction) -> LeafPage * {
  page_id_t new_page_id;
  page_id_t parent_page_id = leaf_page->GetParentPageId();

  auto new_page = buffer_pool_manager->NewPage(&new_page_id);
  if (transaction != nullptr) {
    new_page->WLatch();
    transaction->AddIntoPageSet(new_page);
  }
  auto new_leaf_page = ToLeafPage(new_page);
  new_leaf_page->Init(new_page_id, parent_page_id, leaf_max_size_);
  leaf_page->RedistributeLeafPage(new_leaf_page);
  new_leaf_page->SetNextPageId(leaf_page->GetNextPageId());
  leaf_page->SetNextPageId(new_page_id);
  return new_leaf_page;
}

// TODO(ligch): needed to be reconstructed.
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitInternalPage(InternalPage *internal_page, std::pair<KeyType, page_id_t> &new_item,
                                       BufferPoolManager *buffer_pool_manager, Transaction *transaction)
    -> InternalPage * {
  page_id_t new_page_id;
  page_id_t parent_page_id = internal_page->GetParentPageId();

  auto page = buffer_pool_manager->NewPage(&new_page_id);
  if (transaction != nullptr) {
    page->WLatch();
    transaction->AddIntoPageSet(page);
  }
  auto new_internal_page = ToInternalPage(page);
  new_internal_page->Init(new_page_id, parent_page_id, internal_max_size_);
  internal_page->RedistributeInternalPage(new_internal_page, new_item, buffer_pool_manager, comparator_);

  return new_internal_page;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FetchPage(page_id_t page_id) -> Page * {
  auto page = buffer_pool_manager_->FetchPage(page_id);
  return page;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToTreePage(Page *page) -> BPlusTreePage * {
  return reinterpret_cast<BPlusTreePage *>(page->GetData());
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToLeafPage(Page *page) -> LeafPage * { return reinterpret_cast<LeafPage *>(page->GetData()); }

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToInternalPage(Page *page) -> InternalPage * {
  return reinterpret_cast<InternalPage *>(page->GetData());
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToLeafPage(BPlusTreePage *tree_page) -> LeafPage * {
  return reinterpret_cast<LeafPage *>(tree_page);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToInternalPage(BPlusTreePage *tree_page) -> InternalPage * {
  return reinterpret_cast<InternalPage *>(tree_page);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FetchTreePage(page_id_t page_id) -> BPlusTreePage * {
  auto page = buffer_pool_manager_->FetchPage(page_id);
  return reinterpret_cast<BPlusTreePage *>(page->GetData());
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FetchLeafPage(page_id_t page_id) -> LeafPage * {
  auto page = buffer_pool_manager_->FetchPage(page_id);
  return reinterpret_cast<LeafPage *>(page->GetData());
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FetchInternalPage(page_id_t page_id) -> InternalPage * {
  auto page = buffer_pool_manager_->FetchPage(page_id);
  return reinterpret_cast<InternalPage *>(page->GetData());
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, OpType op_type, Transaction *transaction) -> LeafPage * {
  auto page_id = root_page_id_;
  auto page = FetchPage(page_id);
  auto tree_page = ToTreePage(page);

  if (transaction != nullptr) {
    (op_type == OpType::FIND) ? page->RLatch() : page->WLatch();
    transaction->AddIntoPageSet(page);
  }

  while (!tree_page->IsLeafPage()) {
    auto internal_page = ToInternalPage(page);
    int idx = internal_page->IndexOf(key, comparator_);
    page_id_t child_page_id = internal_page->ValueAt(idx);
    page = FetchPage(child_page_id);
    tree_page = ToTreePage(page);
    if (transaction == nullptr) {
      buffer_pool_manager_->UnpinPage(page_id, false);
    } else {
      (op_type == OpType::FIND) ? page->RLatch() : page->WLatch();
      if (SafeToUnlatchAll(transaction, op_type, tree_page)) {
        UnlatchAllPages(transaction, op_type, false);
      }
      transaction->AddIntoPageSet(page);
    }
    page_id = child_page_id;
  }
  return ToLeafPage(page);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SafeToUnlatchAll(Transaction *transaction, OpType op_type, BPlusTreePage *tree_page) -> bool {
  if (transaction == nullptr) {
    return true;
  }

  if (op_type == OpType::FIND) {
    return true;
  }

  if (op_type == OpType::INSERT) {
    auto cur_size = tree_page->GetSize();
    return tree_page->IsLeafPage() ? cur_size < leaf_max_size_ - 1 : cur_size < internal_max_size_;
  }

  if (op_type == OpType::REMOVE) {
    return tree_page->GetSize() > tree_page->GetMinSize();
  }
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertInParent(BPlusTreePage *old_page, const KeyType &key, BPlusTreePage *new_page,
                                    Transaction *transaction) -> void {
  if (old_page->IsRootPage()) {
    auto new_root_page = buffer_pool_manager_->NewPage(&this->root_page_id_);
    
    if (transaction != nullptr) {
      new_root_page->WLatch();
      transaction->AddIntoPageSet(new_root_page);
    }

    auto root_page = ToInternalPage(new_root_page);
    root_page->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);
    old_page->SetParentPageId(root_page_id_);
    new_page->SetParentPageId(root_page_id_);
    UpdateRootPageId(false);
    root_page->SetupNewRoot(old_page, key, new_page);
    if (transaction == nullptr) {
      buffer_pool_manager_->UnpinPage(root_page_id_, true);
    }
    return;
  }

  auto parent_page_id = old_page->GetParentPageId();
  auto parent_page = FetchInternalPage(parent_page_id);
  if (parent_page->GetSize() < parent_page->GetMaxSize()) {
    parent_page->Insert(key, new_page->GetPageId(), comparator_);
    new_page->SetParentPageId(parent_page_id);
  } else {
    // todo(ligch): fix this
    std::pair<KeyType, page_id_t> new_item{key, new_page->GetPageId()};
    auto new_internal_page = SplitInternalPage(parent_page, new_item, buffer_pool_manager_, transaction);
    InsertInParent(reinterpret_cast<BPlusTreePage *>(parent_page), new_internal_page->KeyAt(0),
                   reinterpret_cast<BPlusTreePage *>(new_internal_page), transaction);
    if (transaction == nullptr) {
      buffer_pool_manager_->UnpinPage(new_internal_page->GetPageId(), true);
    }
  }
  buffer_pool_manager_->UnpinPage(parent_page_id, true);

  // buffer_pool_manager_->UnpinPage(parent_page_id, true);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::CreateNewRoot(const KeyType &key, const ValueType &value, Transaction *transaction) {
  auto new_page = buffer_pool_manager_->NewPage(&root_page_id_);
  if (transaction != nullptr) {
    new_page->WLatch();
    transaction->AddIntoPageSet(new_page);
  }
  auto leaf_page = reinterpret_cast<LeafPage *>(new_page->GetData());
  leaf_page->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
  UpdateRootPageId(false);
  leaf_page->Insert(key, value, comparator_);
  if (transaction == nullptr) {
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
  }
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
  LockTreeRoot(OpType::INSERT, transaction);
  if (IsEmpty()) {
    CreateNewRoot(key, value, transaction);
    UnlatchAllPages(transaction, OpType::INSERT, true);
    return true;
  }

  auto leaf_page = FindLeafPage(key, OpType::INSERT, transaction);
  ValueType v{};
  /* Duplicated key.*/
  if (leaf_page->Find(key, v, comparator_)) {
    if (transaction != nullptr) {
      UnlatchAllPages(transaction, OpType::INSERT, false);
    } else {
      buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    }
    return false;
  }

  leaf_page->Insert(key, value, comparator_);
  if (leaf_page->GetSize() == leaf_page->GetMaxSize()) {
    auto new_leaf_page = SplitLeafPage(leaf_page, buffer_pool_manager_, transaction);
    // Copy up key to parent
    InsertInParent(reinterpret_cast<BPlusTreePage *>(leaf_page), new_leaf_page->KeyAt(0),
                   reinterpret_cast<BPlusTreePage *>(new_leaf_page), transaction);
    if (transaction == nullptr) {
      buffer_pool_manager_->UnpinPage(new_leaf_page->GetPageId(), true);
    }
    // buffer_pool_manager_->UnpinPage(new_leaf_page->GetPageId(), true);
  }

  if (transaction != nullptr) {
    UnlatchAllPages(transaction, OpType::INSERT, true);
  } else {
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
  }
  // buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::CanCoalesce(BPlusTreePage *page, page_id_t &l_page_id, page_id_t &r_page_id,
                                 Transaction *transaction) -> bool {
  if (page->IsRootPage()) {
    return false;
  }
  int cur_size = page->GetSize();
  auto parent_page_id = page->GetParentPageId();
  auto parent_page = FetchInternalPage(parent_page_id);
  auto sep_index = parent_page->ValueIndex(page->GetPageId());
  auto left_sibling_page_id = sep_index > 0 ? parent_page->ValueAt(sep_index - 1) : INVALID_PAGE_ID;

  if (left_sibling_page_id != INVALID_PAGE_ID) {
    auto left_page = FetchPage(left_sibling_page_id);
    auto left_sibling_page = ToTreePage(left_page);
    if (transaction != nullptr) {
      left_page->WLatch();
    }
    if (left_sibling_page->GetSize() + cur_size <= left_sibling_page->GetMaxSize()) {
      l_page_id = left_sibling_page_id;
      r_page_id = page->GetPageId();

      if (transaction != nullptr) {
        transaction->AddIntoPageSet(left_page);
      } else {
        buffer_pool_manager_->UnpinPage(left_sibling_page_id, false);
      }

      buffer_pool_manager_->UnpinPage(parent_page_id, false);
      return true;
    }
    if (transaction != nullptr) {
      left_page->WUnlatch();
    }
    buffer_pool_manager_->UnpinPage(left_sibling_page_id, false);
  }

  auto right_sibling_page_id =
      sep_index < parent_page->GetSize() - 1 ? parent_page->ValueAt(sep_index + 1) : INVALID_PAGE_ID;
  if (right_sibling_page_id != INVALID_PAGE_ID) {
    auto right_page = FetchPage(right_sibling_page_id);
    auto right_sibling_page = ToTreePage(right_page);
    if (transaction != nullptr) {
      right_page->WLatch();
    }
    if (right_sibling_page->GetSize() + cur_size <= right_sibling_page->GetMaxSize()) {
      l_page_id = page->GetPageId();
      r_page_id = right_sibling_page_id;
      if (transaction != nullptr) {
        transaction->AddIntoPageSet(right_page);
      } else {
        buffer_pool_manager_->UnpinPage(right_sibling_page_id, false);
      }
      // buffer_pool_manager_->UnpinPage(right_sibling_page_id, false);
      buffer_pool_manager_->UnpinPage(parent_page_id, false);
      return true;
    }
    if (transaction != nullptr) {
      right_page->WUnlatch();
    }
    buffer_pool_manager_->UnpinPage(right_sibling_page_id, false);
  }
  buffer_pool_manager_->UnpinPage(parent_page_id, false);
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::CanRedistribute(BPlusTreePage *page, int &loc, page_id_t &from_page, Transaction *transaction)
    -> bool {
  if (page->IsRootPage()) {
    return false;
  }

  auto parent_page_id = page->GetParentPageId();
  auto parent_page = FetchInternalPage(parent_page_id);
  auto sep_index = parent_page->ValueIndex(page->GetPageId());
  auto right_sibling_page_id =
      sep_index < parent_page->GetSize() - 1 ? parent_page->ValueAt(sep_index + 1) : INVALID_PAGE_ID;

  if (right_sibling_page_id != INVALID_PAGE_ID) {
    auto right_page = FetchPage(right_sibling_page_id);
    auto right_sibling_page = ToTreePage(right_page);
    if (transaction != nullptr) {
      right_page->WLatch();
    }
    if (right_sibling_page->GetSize() > right_sibling_page->GetMinSize()) {
      loc = 1;
      from_page = right_sibling_page_id;

      if (transaction != nullptr) {
        transaction->AddIntoPageSet(right_page);
      } else {
        buffer_pool_manager_->UnpinPage(right_sibling_page_id, false);
      }
      // buffer_pool_manager_->UnpinPage(right_sibling_page_id, false);
      buffer_pool_manager_->UnpinPage(parent_page_id, false);
      return true;
    }
    if (transaction != nullptr) {
      right_page->WUnlatch();
    }
    buffer_pool_manager_->UnpinPage(right_sibling_page_id, false);

    // buffer_pool_manager_->UnpinPage(right_sibling_page_id, false);
  }

  auto left_sibling_page_id = sep_index > 0 ? parent_page->ValueAt(sep_index - 1) : INVALID_PAGE_ID;
  if (left_sibling_page_id != INVALID_PAGE_ID) {
    auto left_page = FetchPage(left_sibling_page_id);
    auto left_sibling_page = ToTreePage(left_page);
    if (transaction != nullptr) {
      left_page->WLatch();
    }
    if (left_sibling_page->GetSize() > left_sibling_page->GetMinSize()) {
      loc = 0;
      from_page = left_sibling_page_id;
      buffer_pool_manager_->UnpinPage(parent_page_id, false);
      if (transaction != nullptr) {
        transaction->AddIntoPageSet(left_page);
      } else {
        buffer_pool_manager_->UnpinPage(left_sibling_page_id, false);
      }
      // buffer_pool_manager_->UnpinPage(left_sibling_page_id, false);
      return true;
    }
    if (transaction != nullptr) {
      left_page->WUnlatch();
    }
    buffer_pool_manager_->UnpinPage(left_sibling_page_id, false);

    // buffer_pool_manager_->UnpinPage(left_sibling_page_id, false);
  }
  buffer_pool_manager_->UnpinPage(parent_page_id, false);
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DoCoalesce(BPlusTreePage *left_page, BPlusTreePage *right_page, const KeyType &key,
                                Transaction *transaction) {
  page_id_t parent_page_id = left_page->GetParentPageId();
  auto parent_page = FetchInternalPage(parent_page_id);
  int sep_index = parent_page->ValueIndex(right_page->GetPageId());
  KeyType sep_key = parent_page->KeyAt(sep_index);
  //todo(ligch): reconstruct this part
  if (left_page->IsLeafPage()) {
    auto left_leaf_page = ToLeafPage(left_page);
    auto right_leaf_page = ToLeafPage(right_page);
    left_leaf_page->CopyAllFrom(right_leaf_page);
    left_leaf_page->SetNextPageId(right_leaf_page->GetNextPageId());
  } else {
    auto left_internal_page = ToInternalPage(left_page);
    auto right_internal_page = ToInternalPage(right_page);
    right_internal_page->SetKeyAt(0, sep_key);
    left_internal_page->CopyAllFrom(right_internal_page, buffer_pool_manager_);
  }
  buffer_pool_manager_->UnpinPage(parent_page_id, true);
  DeleteEntry(reinterpret_cast<BPlusTreePage *>(parent_page), sep_key, transaction);

}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DoRedistribute(BPlusTreePage *page, int &loc, BPlusTreePage *from_page, const KeyType &key,
                                    Transaction *transaction) {
  page_id_t parent_page_id = page->GetParentPageId();
  auto parent_page = FetchInternalPage(parent_page_id);
  if (loc == 0) {
    /* Borrow from left(prev) sibling.*/
    int sep_index = parent_page->ValueIndex(page->GetPageId());
    if (!page->IsLeafPage()) {
      auto from_internal_page = ToInternalPage(from_page);
      auto to_internal_page = ToInternalPage(page);
      auto from_page_last_index = from_page->GetSize() - 1;
      auto from_key = from_internal_page->KeyAt(from_page_last_index);
      // Using ValueType will report error.
      auto from_value = from_internal_page->ValueAt(from_page_last_index);
      KeyType sep_key = parent_page->KeyAt(sep_index);
      // TODO(ligch): redundant variable, maybe can implement remove_last
      page_id_t fake = 0;
      from_internal_page->Remove(from_key, fake, comparator_);
      to_internal_page->InsertFront(from_key, from_value);
      auto child_page = FetchTreePage(from_value);
      child_page->SetParentPageId(to_internal_page->GetPageId());
      buffer_pool_manager_->UnpinPage(child_page->GetPageId(), true);
      to_internal_page->SetKeyAt(1, sep_key);
      parent_page->SetKeyAt(sep_index, from_key);
    } else {
      auto from_leaf_page = ToLeafPage(from_page);
      auto to_leaf_page = ToLeafPage(page);
      auto from_page_last_index = from_page->GetSize() - 1;
      auto from_key = from_leaf_page->KeyAt(from_page_last_index);
      auto from_value = from_leaf_page->ValueAt(from_page_last_index);
      from_leaf_page->Remove(from_key, from_value, comparator_);
      to_leaf_page->Insert(from_key, from_value, comparator_);
      parent_page->SetKeyAt(sep_index, from_key);
    }
  } else if (loc == 1) {
    /* Borrow from right(next) sibling.*/
    int sep_index = parent_page->ValueIndex(from_page->GetPageId());
    if (!page->IsLeafPage()) {
      auto from_internal_page = ToInternalPage(from_page);
      auto to_internal_page = ToInternalPage(page);
      auto from_key = from_internal_page->KeyAt(1);
      auto from_value = from_internal_page->ValueAt(0);
      KeyType sep_key = parent_page->KeyAt(sep_index);
      from_internal_page->RemoveFront();
      to_internal_page->Insert(sep_key, from_value, comparator_);
      auto child_page = FetchTreePage(from_value);
      child_page->SetParentPageId(to_internal_page->GetPageId());
      buffer_pool_manager_->UnpinPage(child_page->GetPageId(), true);
      parent_page->SetKeyAt(sep_index, from_key);
    } else {
      auto from_leaf_page = ToLeafPage(from_page);
      auto to_leaf_page = ToLeafPage(page);
      auto from_key = from_leaf_page->KeyAt(0);
      auto from_value = from_leaf_page->ValueAt(0);
      to_leaf_page->Insert(from_key, from_value, comparator_);
      from_leaf_page->RemoveFront();
      parent_page->SetKeyAt(sep_index, from_key);
    }
  }
  buffer_pool_manager_->UnpinPage(parent_page_id, true);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DeleteEntry(BPlusTreePage *page, const KeyType &key, Transaction *transaction) {
  //todo(ligch): reconstruct this part
  if (page->IsLeafPage()) {
    ValueType removed_value{};
    auto leaf_page = ToLeafPage(page);
    leaf_page->Remove(key, removed_value, comparator_);
  } else {
    page_id_t removed_page_id{};
    auto internal_page = ToInternalPage(page);
    internal_page->Remove(key, removed_page_id, comparator_);
  }

  if (page->GetSize() < page->GetMinSize()) {
    if (page->IsRootPage()) {
      /* Remove the last item in the tree.*/
      if (page->IsLeafPage()) {
        root_page_id_ = INVALID_PAGE_ID;
        UpdateRootPageId();
        if (transaction != nullptr) {
          transaction->AddIntoDeletedPageSet(page->GetPageId());
        } else {
          buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
          buffer_pool_manager_->DeletePage(page->GetPageId());
        }
        // assert(buffer_pool_manager_->DeletePage(page->GetPageId()));
        return;
      }

      /* Adjust root node. */
      if (page->GetSize() == 1) {
        auto internal_page = ToInternalPage(page);
        root_page_id_ = internal_page->ValueAt(0);
        UpdateRootPageId(false);
        auto new_root_page = FetchInternalPage(root_page_id_);
        // Page *new_root_page = buffer_pool_manager_->FetchPage(root_page_id_);

        // auto new_root = ToInternalPage(new_root_page);
        new_root_page->SetParentPageId(INVALID_PAGE_ID);
        // buffer_pool_manager_->UnpinPage(root_page_id_, true);
        // buffer_pool_manager_->UnpinPage(internal_page->GetPageId(), false);
        buffer_pool_manager_->UnpinPage(root_page_id_, true);
        if (transaction != nullptr) {
          transaction->AddIntoDeletedPageSet(internal_page->GetPageId());
        } else {
          buffer_pool_manager_->UnpinPage(internal_page->GetPageId(), true);
          buffer_pool_manager_->DeletePage(internal_page->GetPageId());
        }
        // buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
        // buffer_pool_manager_->DeletePage(page->GetPageId());
        // assert(buffer_pool_manager_->DeletePage(page->GetPageId()));
      }
      return;
    }


    page_id_t l_page_id = -1;
    page_id_t r_page_id = -1;
    int loc = -1;
    page_id_t from_page_id = -1;

    // buffer_pool_manager_->GetPinCount(page->GetPageId());
    if (CanCoalesce(page, l_page_id, r_page_id, transaction)) {
      auto left_page = FetchTreePage(l_page_id);
      auto right_page = FetchTreePage(r_page_id);
      DoCoalesce(left_page, right_page, key, transaction);
      buffer_pool_manager_->UnpinPage(l_page_id, true);
      buffer_pool_manager_->UnpinPage(r_page_id, true);
      if (transaction != nullptr) {
        transaction->AddIntoDeletedPageSet(r_page_id);
      } else {
        buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
        buffer_pool_manager_->DeletePage(r_page_id);
      }
      // assert(buffer_pool_manager_->DeletePage(page->GetPageId()));
    } else if (CanRedistribute(page, loc, from_page_id, transaction)) {
      auto from_page = FetchTreePage(from_page_id);
      DoRedistribute(page, loc, from_page, key, transaction);
      buffer_pool_manager_->UnpinPage(from_page_id, true);
      if (transaction == nullptr) {
        buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
      }
    }
  } else {
    if (transaction == nullptr) {
      buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    }
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  LockTreeRoot(OpType::REMOVE, transaction);

  if (IsEmpty()) {
    UnlatchAllPages(transaction, OpType::REMOVE, false);
    return;
  }

  auto leaf_page = FindLeafPage(key, OpType::REMOVE, transaction);
  ValueType v{};
  if (!leaf_page->Find(key, v, comparator_)) {
    if (transaction != nullptr) {
      UnlatchAllPages(transaction, OpType::REMOVE, false);
    } else {
      buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    }
    return;
  }

  // buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
  DeleteEntry(reinterpret_cast<BPlusTreePage *>(leaf_page), key, transaction);
  UnlatchAllPages(transaction, OpType::REMOVE, true);
  DeleteAllPages(transaction);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeftmostLeafPage() -> LeafPage * {
  auto page = FetchTreePage(root_page_id_);
  while (!page->IsLeafPage()) {
    auto internal_page = reinterpret_cast<InternalPage *>(page);
    page_id_t child_page_id = internal_page->ValueAt(0);
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    page = FetchTreePage(child_page_id);
  }
  return reinterpret_cast<LeafPage *>(page);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindRightmostLeafPage() -> LeafPage * {
  auto page = FetchTreePage(root_page_id_);
  while (!page->IsLeafPage()) {
    auto internal_page = reinterpret_cast<InternalPage *>(page);
    page_id_t child_page_id = internal_page->ValueAt(internal_page->GetSize() - 1);
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    page = FetchTreePage(child_page_id);
  }
  return reinterpret_cast<LeafPage *>(page);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UnlatchAllPages(Transaction *transaction, OpType op_type, bool dirty) {
  if (transaction == nullptr) {
    return;
  }

  auto page_queue = transaction->GetPageSet();
  while (!page_queue->empty()) {
    auto cur_page = page_queue->front();
    page_queue->pop_front();
    if (cur_page == nullptr) {
      UnlockTreeRoot(op_type);
      continue;
    }
    auto cur_page_id = cur_page->GetPageId();
    (op_type == OpType::FIND) ? cur_page->RUnlatch() : cur_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(cur_page_id, dirty);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DeleteAllPages(Transaction *transaction) {
  if (transaction == nullptr) {
    return;
  }
  auto deleted_pages = transaction->GetDeletedPageSet();
  for (auto page_id : *deleted_pages) {
    buffer_pool_manager_->DeletePage(page_id);
  }
}
/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  auto leaf_page = FindLeftmostLeafPage();
  // buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
  return INDEXITERATOR_TYPE(leaf_page, 0, buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  auto leaf_page = FindLeafPage(key, OpType::FIND, nullptr);
  int index = leaf_page->IndexOf(key, comparator_);
  // buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
  return INDEXITERATOR_TYPE(leaf_page, index, buffer_pool_manager_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(nullptr, 0, buffer_pool_manager_); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
inline void BPLUSTREE_TYPE::LockTreeRoot(OpType op_type, Transaction *transaction) {
  if (transaction == nullptr) {
    return;
  }

  op_type == OpType::FIND ? tree_latch_.RLock() : tree_latch_.WLock();
  transaction->AddIntoPageSet(nullptr);
  // std::cout << "lock the root" << std::endl;
}

INDEX_TEMPLATE_ARGUMENTS
inline void BPLUSTREE_TYPE::UnlockTreeRoot(OpType op_type) {
  op_type == OpType::FIND ? tree_latch_.RUnlock() : tree_latch_.WUnlock();
  // std::cout << "unlock the root" << std::endl;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
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

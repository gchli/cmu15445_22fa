//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      iterator_(dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(
                    exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid())->index_.get())
                    ->GetBeginIterator()) {}

void IndexScanExecutor::Init() {
  IndexInfo *index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  tree_ = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info->index_.get());
  iterator_ = tree_->GetBeginIterator();
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iterator_ == tree_->GetEndIterator()) {
    return false;
  }
  auto index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  auto table_info = exec_ctx_->GetCatalog()->GetTable(index_info->table_name_);
  *rid = (*iterator_).second;
  table_info->table_->GetTuple(*rid, tuple, exec_ctx_->GetTransaction());
  ++iterator_;
  return true;
}
}  // namespace bustub

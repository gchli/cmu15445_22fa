//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include "common/exception.h"
#include "concurrency/lock_manager.h"
#include "concurrency/transaction.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      iterator_(exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_->Begin(exec_ctx_->GetTransaction())) {}

void SeqScanExecutor::Init() {
  auto iso_level = exec_ctx_->GetTransaction()->GetIsolationLevel();
  if (iso_level != IsolationLevel::READ_UNCOMMITTED) {
    try {
      bool locked = exec_ctx_->GetLockManager()->LockTable(
          exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_SHARED, plan_->GetTableOid());
      if (!locked) {
        throw ExecutionException("SeqScanExecutor failed to lock table");
      }
    } catch (TransactionAbortException &e) {
      throw ExecutionException(e.what());
    }
  }
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto oid = plan_->GetTableOid();
  auto table_info = exec_ctx_->GetCatalog()->GetTable(oid);
  const auto &txn = exec_ctx_->GetTransaction();
  auto iso_level = txn->GetIsolationLevel();
  const auto &lock_manager = exec_ctx_->GetLockManager();
  if (iterator_ == table_info->table_->End()) {
    if (iso_level == IsolationLevel::READ_COMMITTED) {
      table_oid_t oid = table_info->oid_;
      // const auto &row_lock_set = txn->GetSharedRowLockSet()->at(oid);
      // for (const auto &rid: row_lock_set) {
      //   lock_manager->UnlockRow(exec_ctx_->GetTransaction(), oid, rid);
      // }
      lock_manager->UnlockTable(exec_ctx_->GetTransaction(), oid);
    }
    return false;
  }

  *rid = iterator_->GetRid();
  if (iso_level != IsolationLevel::READ_UNCOMMITTED) {
    try {
      if (!txn->IsRowExclusiveLocked(table_info->oid_, *rid)) {
        bool locked =
            exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::SHARED, oid, *rid);
        if (!locked) {
          throw ExecutionException("SeqScanExecutor failed to lock row");
        }
      }
    } catch (TransactionAbortException &e) {
      throw ExecutionException(e.what());
    }
  }

  *tuple = *iterator_;
  ++iterator_;
  if (iso_level == IsolationLevel::READ_COMMITTED) {
    if (txn->IsRowSharedLocked(table_info->oid_, *rid)) {
      lock_manager->UnlockRow(exec_ctx_->GetTransaction(), table_info->oid_, *rid);
    }
  }
  return true;
}

}  // namespace bustub

//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  child_executor_->Init();
  try {
    bool locked = exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(),
                                                         LockManager::LockMode::INTENTION_EXCLUSIVE, plan_->TableOid());
    if (!locked) {
      throw ExecutionException("Delete Executor Get Table Lock Failed");
    }
  } catch (TransactionAbortException &e) {
    throw ExecutionException("Delete Executor Get Table Lock Failed");
  }
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  Tuple child_tuple;

  std::vector<Value> values{};
  int count = 0;
  bool status = child_executor_->Next(&child_tuple, rid);
  values.reserve(GetOutputSchema().GetColumnCount());
  values.emplace_back(TypeId::INTEGER, 0);
  *tuple = Tuple(values, &GetOutputSchema());
  if (not_end_ && !status) {
    not_end_ = false;
    return true;
  }
  not_end_ = false;
  if (!status) {
    return false;
  }
  while (status) {
    auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
    try {
      bool locked = exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::EXCLUSIVE,
                                                         table_info->oid_, *rid);
      if (!locked) {
        throw ExecutionException("Delete Executor Get Row Lock Failed");
      }
    } catch (TransactionAbortException &e) {
      throw ExecutionException("Delete Executor Get Row Lock Failed");
    }
    bool delete_status = table_info->table_->MarkDelete(*rid, exec_ctx_->GetTransaction());
    if (delete_status) {
      ++count;
      auto index_infos = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
      for (const auto &index_info : index_infos) {
        std::vector<Value> key_values{};
        key_values.reserve(index_info->key_schema_.GetColumnCount());
        for (auto &column_idx : index_info->index_->GetKeyAttrs()) {
          key_values.emplace_back(child_tuple.GetValue(&child_executor_->GetOutputSchema(), column_idx));
        }
        Tuple index_tuple = Tuple(key_values, &index_info->key_schema_);
        index_info->index_->DeleteEntry(index_tuple, *rid, exec_ctx_->GetTransaction());
      }
    }
    status = child_executor_->Next(&child_tuple, rid);
  }

  //  values.reserve(GetOutputSchema().GetColumnCount());
  //  values.emplace_back(Value{TypeId::INTEGER, count});
  values[0] = Value{TypeId::INTEGER, count};
  *tuple = Tuple(values, &GetOutputSchema());
  return true;
}

}  // namespace bustub

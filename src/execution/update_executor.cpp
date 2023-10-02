//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/update_executor.h"
#include "type/value.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_info_(exec_ctx->GetCatalog()->GetTable(plan_->TableOid())),
      child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() {
  child_executor_->Init();
  try {
    bool locked = exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(),
                                                         LockManager::LockMode::INTENTION_EXCLUSIVE, table_info_->oid_);
    if (!locked) {
      throw ExecutionException("UpdateExecutor LockTable Failed");
    }
  } catch (TransactionAbortException &e) {
    throw ExecutionException(e.what());
  }
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (!not_end_) {
    return false;
  }
  Tuple child_tuple;
  bool status = child_executor_->Next(&child_tuple, rid);
  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());
  values.emplace_back(TypeId::INTEGER, 0);
  *tuple = Tuple(values, &GetOutputSchema());
  int count = 0;

  if (!status) {
    not_end_ = false;
    return true;
  }

  while (status) {
    try {
      bool locked = exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::EXCLUSIVE,
                                                         table_info_->oid_, *rid);
      if (!locked) {
        throw ExecutionException("UpdateExecutor LockRow");
      }
    } catch (TransactionAbortException &e) {
      throw ExecutionException(e.what());
    }
    std::vector<Value> new_values;
    new_values.reserve(child_executor_->GetOutputSchema().GetColumnCount());
    for (const auto &expr : plan_->target_expressions_) {
      new_values.push_back(expr->Evaluate(&child_tuple, child_executor_->GetOutputSchema()));
    }
    auto new_tuple = Tuple(new_values, &child_executor_->GetOutputSchema());
    bool update_ok = table_info_->table_->UpdateTuple(new_tuple, *rid, exec_ctx_->GetTransaction());
    if (update_ok) {
      ++count;
    }
    status = child_executor_->Next(&child_tuple, rid);
  }
  values[0] = Value{TypeId::INTEGER, count};
  *tuple = Tuple(values, &GetOutputSchema());
  not_end_ = false;
  return true;
}

}  // namespace bustub

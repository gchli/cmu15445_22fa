//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  child_executor_->Init();
  //	throw NotImplementedException("InsertExecutor is not implemented");
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  Tuple child_tuple;
  bool status = child_executor_->Next(&child_tuple, rid);
	std::vector<Value> values{};
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
  int count = 0;
  while (status) {
    ++count;
    auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
    bool insert_status = table_info->table_->InsertTuple(child_tuple, rid, exec_ctx_->GetTransaction());
    (void)insert_status;
    auto index_infos = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
    for (auto &index_info : index_infos) {
      std::vector<Value> key_values{};
      key_values.reserve(index_info->key_schema_.GetColumnCount());
      for (auto &column_idx : index_info->index_->GetKeyAttrs()) {
        key_values.emplace_back(child_tuple.GetValue(&child_executor_->GetOutputSchema(), column_idx));
      }
      Tuple index_tuple = Tuple(key_values, &index_info->key_schema_);
      index_info->index_->InsertEntry(index_tuple, *rid, exec_ctx_->GetTransaction());
    }
    status = child_executor_->Next(&child_tuple, rid);
  }

  values[0] = Value{TypeId::INTEGER, count};
  *tuple = Tuple(values, &GetOutputSchema());
  return true;
}

}  // namespace bustub

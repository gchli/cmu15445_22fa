//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include "type/value_factory.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestIndexJoinExecutor::Init() {
  child_executor_->Init();
  index_ = 0;
  IndexInfo *index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  TableInfo *table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetInnerTableOid());
  Tuple child_tuple;
  RID rid;
  auto status = child_executor_->Next(&child_tuple, &rid);
  while (status) {
    Tuple key{};
    std::vector<Value> key_values{};
    for (size_t i = 0; i < index_info->key_schema_.GetColumnCount(); ++i) {
      key_values.push_back(plan_->KeyPredicate()->Evaluate(&child_tuple, child_executor_->GetOutputSchema()));
    }
    std::vector<RID> index_result;
    index_info->index_->ScanKey(Tuple(key_values, &index_info->key_schema_), &index_result,
                                exec_ctx_->GetTransaction());
    std::vector<Value> values;
    for (size_t i = 0; i < child_executor_->GetOutputSchema().GetColumnCount(); ++i) {
      values.push_back(child_tuple.GetValue(&child_executor_->GetOutputSchema(), i));
    }
    for (auto &r : index_result) {
      Tuple right_tuple{};
      std::vector<Value> all_values(values);
      table_info->table_->GetTuple(r, &right_tuple, exec_ctx_->GetTransaction());
      for (size_t i = 0; i < plan_->InnerTableSchema().GetColumnCount(); ++i) {
        all_values.emplace_back(right_tuple.GetValue(&plan_->InnerTableSchema(), i));
      }
      result_.emplace_back(all_values, &GetOutputSchema());
    }

    if (index_result.empty() && plan_->GetJoinType() == JoinType::LEFT) {
      for (size_t i = 0; i < plan_->InnerTableSchema().GetColumnCount(); ++i) {
        values.emplace_back(ValueFactory::GetNullValueByType(TypeId::INTEGER));
      }
      result_.emplace_back(values, &GetOutputSchema());
    }

    status = child_executor_->Next(&child_tuple, &rid);
  }
}

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (index_ == result_.size()) {
    return false;
  }
  *tuple = result_[index_++];
  return true;
}

}  // namespace bustub

//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(SimpleAggregationHashTable(plan_->GetAggregates(), plan_->GetAggregateTypes())),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  child_->Init();
  Tuple child_tuple;
  RID rid;
  while (child_->Next(&child_tuple, &rid)) {
    AggregateKey keys = MakeAggregateKey(&child_tuple);
    AggregateValue values = MakeAggregateValue(&child_tuple);
    aht_.InsertCombine(keys, values);
  }
  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  std::vector<Value> values;
  if (aht_.Begin() == aht_.End() && not_end_) {
    not_end_ = false;
    if (GetOutputSchema().GetColumnCount() == aht_.GenerateInitialAggregateValue().aggregates_.size()) {
      for (auto &value : aht_.GenerateInitialAggregateValue().aggregates_) {
        values.push_back(value);
      }
    } else {
      return false;
    }

    *tuple = Tuple(values, &GetOutputSchema());
    return true;
  }
  if (aht_iterator_ == aht_.End() && !not_end_) {
    return false;
  }
  not_end_ = false;
  values.reserve(GetOutputSchema().GetColumnCount());
  for (const auto &key : aht_iterator_.Key().group_bys_) {
    values.emplace_back(key);
  }
  auto agg_values = aht_iterator_.Val();
  for (auto &value : agg_values.aggregates_) {
    values.push_back(value);
  }
  if (values.empty()) {
    for (const auto &value : aht_.GenerateInitialAggregateValue().aggregates_) {
      values.push_back(value);
    }
  }

  *tuple = Tuple(values, &GetOutputSchema());
  ++aht_iterator_;
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub

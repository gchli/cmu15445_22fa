#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_executor_->Init();
  Tuple child_tuple;
  RID rid;
  while (child_executor_->Next(&child_tuple, &rid)) {
    tuples_.emplace_back(child_tuple);
  }
  auto order_by = plan_->GetOrderBy();
  auto cmp = [&](const Tuple &a, const Tuple &b) {
    for (const auto &[order_by_type, exp_ref] : order_by) {
      Value a_v = exp_ref->Evaluate(&a, child_executor_->GetOutputSchema());
      Value b_v = exp_ref->Evaluate(&b, child_executor_->GetOutputSchema());
      if (a_v.CompareEquals(b_v) != CmpBool::CmpFalse) {
        continue;
      }
      if (order_by_type == OrderByType::DEFAULT || order_by_type == OrderByType::ASC) {
        return a_v.CompareLessThan(b_v) == CmpBool::CmpTrue;
      }
      return a_v.CompareGreaterThan(b_v) == CmpBool::CmpTrue;
    }
    return false;
  };
  std::sort(tuples_.begin(), tuples_.end(), cmp);
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (index_ >= tuples_.size()) {
    return false;
  }
  *tuple = tuples_[index_++];
  return true;
}

}  // namespace bustub

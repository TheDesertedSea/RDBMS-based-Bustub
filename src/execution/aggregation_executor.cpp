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
#include "common/rid.h"
#include "execution/plans/aggregation_plan.h"
#include "storage/table/tuple.h"

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  child_executor_->Init();
  aht_.Clear();

  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    aht_.InsertCombine(MakeAggregateKey(&tuple), MakeAggregateValue(&tuple));
  }

  aht_iterator_ = aht_.Begin();
  if (aht_iterator_ == aht_.End()) {
    if (plan_->GetGroupBys().empty()) {
      aht_.AddEmptyEntry();
      aht_iterator_ = aht_.Begin();
    }
  }
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (aht_iterator_ == aht_.End()) {
    return false;
  }

  auto aggregate_key = aht_iterator_.Key();
  auto aggregate_value = aht_iterator_.Val();
  ++aht_iterator_;

  // Construct the output tuple
  std::vector<Value> values;

  for (const auto &key : aggregate_key.group_bys_) {
    values.emplace_back(key);
  }

  for (const auto &val : aggregate_value.aggregates_) {
    values.emplace_back(val);
  }

  *tuple = Tuple(values, &plan_->OutputSchema());
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub

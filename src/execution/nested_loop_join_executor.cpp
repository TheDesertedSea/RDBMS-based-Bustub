//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include <cstdint>
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();

  is_first_ = true;
  has_matched_right_ = false;
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  switch (plan_->GetJoinType()) {
    case JoinType::LEFT:
      return NextLeftJoin(tuple, rid);
    case JoinType::INNER:
      return NextInnerJoin(tuple, rid);
    default:
      throw NotImplementedException("Join type not supported.");
  }
}

auto NestedLoopJoinExecutor::NextLeftJoin(Tuple *tuple, RID *rid) -> bool {
  if (is_first_) {
    is_first_ = false;
    if (!left_executor_->Next(&left_tuple_, &left_rid_)) {
      return false;
    }
  }
  auto left_output_schema = left_executor_->GetOutputSchema();

  Tuple right_tuple;
  RID right_rid;
  auto right_output_schema = right_executor_->GetOutputSchema();
  bool found = false;
  while (right_executor_->Next(&right_tuple, &right_rid)) {
    if (plan_->Predicate()
            ->EvaluateJoin(&left_tuple_, left_output_schema, &right_tuple, right_output_schema)
            .GetAs<bool>()) {
      found = true;
      has_matched_right_ = true;
      break;
    }
  }

  if (!found) {
    if (has_matched_right_) {
      right_executor_->Init();
      is_first_ = true;
      has_matched_right_ = false;
      return NextLeftJoin(tuple, rid);
    }

    std::vector<Value> values;
    for (uint32_t i = 0; i < left_output_schema.GetColumnCount(); i++) {
      values.push_back(left_tuple_.GetValue(&left_output_schema, i));
    }
    // Fill the right side with NULL
    for (uint32_t i = 0; i < right_output_schema.GetColumnCount(); i++) {
      values.push_back(ValueFactory::GetNullValueByType(right_output_schema.GetColumns()[i].GetType()));
    }
    *tuple = Tuple(values, &plan_->OutputSchema());

    right_executor_->Init();
    is_first_ = true;
    return true;
  }

  // Construct the output tuple
  std::vector<Value> values;
  for (uint32_t i = 0; i < left_output_schema.GetColumnCount(); i++) {
    values.push_back(left_tuple_.GetValue(&left_output_schema, i));
  }
  for (uint32_t i = 0; i < right_output_schema.GetColumnCount(); i++) {
    values.push_back(right_tuple.GetValue(&right_output_schema, i));
  }
  *tuple = Tuple(values, &plan_->OutputSchema());

  return true;
}

auto NestedLoopJoinExecutor::NextInnerJoin(Tuple *tuple, RID *rid) -> bool {
  if (is_first_) {
    is_first_ = false;
    if (!left_executor_->Next(&left_tuple_, &left_rid_)) {
      return false;
    }
  }
  auto left_output_schema = left_executor_->GetOutputSchema();

  Tuple right_tuple;
  RID right_rid;
  auto right_output_schema = right_executor_->GetOutputSchema();
  bool found = false;
  while (right_executor_->Next(&right_tuple, &right_rid)) {
    if (plan_->Predicate()
            ->EvaluateJoin(&left_tuple_, left_output_schema, &right_tuple, right_output_schema)
            .GetAs<bool>()) {
      found = true;
      break;
    }
  }

  if (!found) {
    right_executor_->Init();
    is_first_ = true;
    return NextInnerJoin(tuple, rid);
  }

  // Construct the output tuple
  std::vector<Value> values;
  for (uint32_t i = 0; i < left_output_schema.GetColumnCount(); i++) {
    values.push_back(left_tuple_.GetValue(&left_output_schema, i));
  }
  for (uint32_t i = 0; i < right_output_schema.GetColumnCount(); i++) {
    values.push_back(right_tuple.GetValue(&right_output_schema, i));
  }
  *tuple = Tuple(values, &plan_->OutputSchema());

  return true;
}

}  // namespace bustub

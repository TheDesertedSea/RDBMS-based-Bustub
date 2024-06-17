//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "type/value_factory.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_child)),
      right_executor_(std::move(right_child)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();

  hash_table_.clear();

  // Add all tuples from the left table to the hash table
  Tuple tuple;
  RID rid;
  while (left_executor_->Next(&tuple, &rid)) {
    auto key = MakeHashJoinKeyLeft(&tuple);
    if (hash_table_.find(key) == hash_table_.end()) {
      hash_table_[key] = {HashJoinLeftValueInfo{tuple, false}};
    } else {
      hash_table_[key].emplace_back(tuple, false);
    }
  }

  matched_ = false;
  start_null_matching_ = false;
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  switch (plan_->GetJoinType()) {
    case JoinType::LEFT:
      return NextLeftJoin(tuple, rid);
    case JoinType::INNER:
      return NextInnerJoin(tuple, rid);
    default:
      throw NotImplementedException("Join type not supported.");
  }
}

auto HashJoinExecutor::NextInnerJoin(Tuple *tuple, RID *rid) -> bool {
  if (matched_) {
    // has a matched right tuple
    if (value_info_list_iterator_ == hash_table_iterator_->second.end()) {
      // current matched value info list is exhausted
      matched_ = false;
      return NextInnerJoin(tuple, rid);
    }
    // Get one matched left tuple from the value info list

    auto left_tuple = value_info_list_iterator_->tuple_;
    value_info_list_iterator_->has_matched_right_ = true;
    ++value_info_list_iterator_;

    std::vector<Value> values;
    auto left_output_schema = left_executor_->GetOutputSchema();
    for (uint32_t i = 0; i < left_output_schema.GetColumnCount(); i++) {
      values.push_back(left_tuple.GetValue(&left_output_schema, i));
    }
    auto right_output_schema = right_executor_->GetOutputSchema();
    for (uint32_t i = 0; i < right_output_schema.GetColumnCount(); i++) {
      values.push_back(cur_matched_right_tuple_.GetValue(&right_output_schema, i));
    }
    *tuple = Tuple(values, &plan_->OutputSchema());

    return true;
  }
  // Find the next matched tuple

  Tuple right_tuple;
  RID right_rid;

  while (right_executor_->Next(&right_tuple, &right_rid)) {
    auto right_key = MakeHashJoinKeyRight(&right_tuple);
    hash_table_iterator_ = hash_table_.find(right_key);
    if (hash_table_iterator_ != hash_table_.end()) {
      value_info_list_iterator_ = hash_table_iterator_->second.begin();
      cur_matched_right_tuple_ = right_tuple;
      matched_ = true;
      return NextInnerJoin(tuple, rid);
    }
  }

  return false;
}

auto HashJoinExecutor::NextLeftJoin(Tuple *tuple, RID *rid) -> bool {
  if (start_null_matching_) {
    // Current in the null matching phase
    if (hash_table_iterator_ == hash_table_.end()) {
      // hash table is exhausted
      return false;
    }

    // Find the next unmatched left tuple
    while ((value_info_list_iterator_ != hash_table_iterator_->second.end())) {
      auto &value_info = *value_info_list_iterator_;
      ++value_info_list_iterator_;
      if (!value_info.has_matched_right_) {
        auto left_tuple = value_info.tuple_;

        std::vector<Value> values;
        auto left_output_schema = left_executor_->GetOutputSchema();
        for (uint32_t i = 0; i < left_output_schema.GetColumnCount(); i++) {
          values.push_back(left_tuple.GetValue(&left_output_schema, i));
        }
        // Fill the right side with NULL
        for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); i++) {
          values.push_back(ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(i).GetType()));
        }
        *tuple = Tuple(values, &plan_->OutputSchema());
        return true;
      }
    }

    ++hash_table_iterator_;
    if (hash_table_iterator_ == hash_table_.end()) {
      return false;
    }
    value_info_list_iterator_ = hash_table_iterator_->second.begin();
    return NextLeftJoin(tuple, rid);
  }

  if (matched_) {
    // has a matched right tuple
    if (value_info_list_iterator_ == hash_table_iterator_->second.end()) {
      // current matched tuple list is exhausted
      matched_ = false;
      return NextLeftJoin(tuple, rid);
    }
    // Get one matched tuple from the list

    auto left_tuple = value_info_list_iterator_->tuple_;
    value_info_list_iterator_->has_matched_right_ = true;
    ++value_info_list_iterator_;

    std::vector<Value> values;
    auto left_output_schema = left_executor_->GetOutputSchema();
    for (uint32_t i = 0; i < left_output_schema.GetColumnCount(); i++) {
      values.push_back(left_tuple.GetValue(&left_output_schema, i));
    }
    auto right_output_schema = right_executor_->GetOutputSchema();
    for (uint32_t i = 0; i < right_output_schema.GetColumnCount(); i++) {
      values.push_back(cur_matched_right_tuple_.GetValue(&right_output_schema, i));
    }
    *tuple = Tuple(values, &plan_->OutputSchema());

    return true;
  }

  // Find the next matched tuple
  Tuple right_tuple;
  RID right_rid;

  while (right_executor_->Next(&right_tuple, &right_rid)) {
    auto right_key = MakeHashJoinKeyRight(&right_tuple);
    hash_table_iterator_ = hash_table_.find(right_key);
    if (hash_table_iterator_ != hash_table_.end()) {
      value_info_list_iterator_ = hash_table_iterator_->second.begin();
      cur_matched_right_tuple_ = right_tuple;
      matched_ = true;
      return NextLeftJoin(tuple, rid);
    }
  }

  // No matched tuple found, start null matching
  hash_table_iterator_ = hash_table_.begin();
  if (hash_table_iterator_ == hash_table_.end()) {
    return false;
  }
  value_info_list_iterator_ = hash_table_iterator_->second.begin();
  start_null_matching_ = true;

  return NextLeftJoin(tuple, rid);
}

auto HashJoinExecutor::MakeHashJoinKeyLeft(const Tuple *tuple) -> HashJoinKey {
  std::vector<Value> keys;
  for (const auto &expr : plan_->LeftJoinKeyExpressions()) {
    keys.emplace_back(expr->Evaluate(tuple, left_executor_->GetOutputSchema()));
  }
  return {keys};
}

auto HashJoinExecutor::MakeHashJoinKeyRight(const Tuple *tuple) -> HashJoinKey {
  std::vector<Value> keys;
  for (const auto &expr : plan_->RightJoinKeyExpressions()) {
    keys.emplace_back(expr->Evaluate(tuple, right_executor_->GetOutputSchema()));
  }
  return {keys};
}

}  // namespace bustub

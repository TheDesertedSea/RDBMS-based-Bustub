#include "execution/executors/window_function_executor.h"
#include <cstdint>
#include "binder/bound_order_by.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

WindowFunctionExecutor::WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void WindowFunctionExecutor::Init() {
  child_executor_->Init();

  // Initialize the hash tables for each window function
  for (auto &func : plan_->window_functions_) {
    uint32_t index = func.first;
    auto &window_func = func.second;
    window_hash_tables_[index] =
        std::make_unique<SimpleWindowHashTable>(window_func.partition_by_, window_func.function_, window_func.type_);
  }

  auto &order_by_infos = plan_->window_functions_.begin()->second.order_by_;
  if (!order_by_infos.empty()) {
    // If there is an order by clause, we need to sort the input tuples
    has_order_by_ = true;
    Tuple tuple;
    RID rid;
    while (child_executor_->Next(&tuple, &rid)) {
      WindowSortEntry entry;
      entry.tuple_ = tuple;
      entry.rid_ = rid;

      for (const auto &order_by_info : order_by_infos) {
        entry.keys_.push_back(order_by_info.second->Evaluate(&entry.tuple_, child_executor_->GetOutputSchema()));
      }

      window_sort_entries_.push_back(entry);
    }

    // Sort the entries based on the order by clause
    std::vector<OrderByType> order_by_types(order_by_infos.size());
    for (size_t i = 0; i < order_by_infos.size(); i++) {
      order_by_types[i] = order_by_infos[i].first;
    }
    std::sort(window_sort_entries_.begin(), window_sort_entries_.end(), WindowSortComparator(order_by_types));
    window_sort_iter_ = window_sort_entries_.begin();
  } else {
    // If there is no order by clause, we can immediately compute the window functions
    has_order_by_ = false;

    Tuple tuple;
    RID rid;
    while (child_executor_->Next(&tuple, &rid)) {
      for (auto &func : plan_->window_functions_) {
        window_hash_tables_[func.first]->InsertCombine(tuple, child_executor_->GetOutputSchema());
      }
      original_tuples_.push_back(tuple);
    }

    original_tuple_iter_ = original_tuples_.begin();
  }
}

auto WindowFunctionExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (has_order_by_) {
    // If there is an order by clause, we need to iterate over the sorted entries
    if (window_sort_iter_ == window_sort_entries_.end()) {
      return false;
    }

    std::vector<Value> output_values(plan_->columns_.size());
    for (size_t col_idx = 0; col_idx != plan_->columns_.size(); col_idx++) {
      if (window_hash_tables_.find(col_idx) != window_hash_tables_.end()) {
        // Insert and combine window function value, and get the current result
        auto &window_hash_table = window_hash_tables_[col_idx];
        WindowFunctionValue window_val =
            window_hash_table->InsertCombine(window_sort_iter_->tuple_, child_executor_->GetOutputSchema());
        if (window_hash_table->GetType() == WindowFunctionType::Rank) {
          if (IsEqualToLastEntry(*window_sort_iter_)) {
            // If the current tuple is equal to the last tuple, we should output the last rank value
            output_values[col_idx] = last_rank_values_[col_idx];
          } else {
            output_values[col_idx] = window_val.value_;
          }
          // Update the last rank value
          last_rank_values_[col_idx] = output_values[col_idx];
        } else {
          output_values[col_idx] = window_val.value_;
        }
      } else {
        // Column value
        output_values[col_idx] = window_sort_iter_->tuple_.GetValue(&child_executor_->GetOutputSchema(), col_idx);
      }
    }

    last_entry_ = &*window_sort_iter_;  // Save the last entry
    *tuple = Tuple(output_values, &plan_->OutputSchema());
    ++window_sort_iter_;
    return true;
  }
  // !has_order_by_

  // No order by, iterate over the original tuples and get the window function values
  if (original_tuple_iter_ == original_tuples_.end()) {
    return false;
  }

  std::vector<Value> output_values(plan_->columns_.size());
  for (size_t col_idx = 0; col_idx != plan_->columns_.size(); col_idx++) {
    if (window_hash_tables_.find(col_idx) != window_hash_tables_.end()) {
      // window function value
      output_values[col_idx] =
          window_hash_tables_[col_idx]->GetResult(*original_tuple_iter_, child_executor_->GetOutputSchema()).value_;
    } else {
      // Column value
      output_values[col_idx] = original_tuple_iter_->GetValue(&child_executor_->GetOutputSchema(), col_idx);
    }
  }

  *tuple = Tuple(output_values, &plan_->OutputSchema());
  ++original_tuple_iter_;
  return true;
}
}  // namespace bustub

//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// window_function_executor.h
//
// Identification: src/include/execution/executors/window_function_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <unordered_map>
#include <vector>

#include "catalog/schema.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * Keys for partition bys
 */
struct WindowFunctionKey {
  std::vector<Value> keys_;

  auto operator==(const WindowFunctionKey &other) const -> bool {
    for (uint32_t i = 0; i < other.keys_.size(); i++) {
      if (keys_[i].CompareEquals(other.keys_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};

/**
 * Aggregation value of a window function
 */
struct WindowFunctionValue {
  Value value_;
};

}  // namespace bustub

namespace std {

/** Implements std::hash on WindowFunctionKey */
template <>
struct hash<bustub::WindowFunctionKey> {
  auto operator()(const bustub::WindowFunctionKey &key) const -> std::size_t {
    size_t curr_hash = 0;
    for (const auto &key : key.keys_) {
      if (!key.IsNull()) {
        curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&key));
      }
    }
    return curr_hash;
  }
};

}  // namespace std

namespace bustub {

/**
 * Hash table for a window function
 */
class SimpleWindowHashTable {
 public:
  SimpleWindowHashTable(const std::vector<AbstractExpressionRef> &partition_bys, const AbstractExpressionRef &function,
                        const WindowFunctionType function_type)
      : partition_bys_(partition_bys), function_{function}, function_type_{function_type} {}

  /** @return The initial aggregate value for this aggregation executor */
  auto GenerateInitialWindowFunctionValue() -> WindowFunctionValue {
    Value value;
    switch (function_type_) {
      case WindowFunctionType::CountStarAggregate:
        // Count start starts at zero.
        value = ValueFactory::GetIntegerValue(0);
        break;
      case WindowFunctionType::CountAggregate:
      case WindowFunctionType::SumAggregate:
      case WindowFunctionType::MinAggregate:
      case WindowFunctionType::MaxAggregate:
        // Others starts at null.
        value = ValueFactory::GetNullValueByType(TypeId::INTEGER);
        break;
      case WindowFunctionType::Rank:
        value = ValueFactory::GetIntegerValue(0);
        break;
    }
    return {value};
  }

  /**
   * TODO(Student)
   *
   * Combines the input into the aggregation result.
   * @param[out] result The output aggregate value
   * @param input The input value
   */
  void CombineWindowFunctionValues(WindowFunctionValue *result, const WindowFunctionValue &input) {
    switch (function_type_) {
      case WindowFunctionType::CountStarAggregate:
        result->value_ = result->value_.Add(ValueFactory::GetIntegerValue(1));
        break;
      case WindowFunctionType::CountAggregate:
        if (!input.value_.IsNull()) {
          if (result->value_.IsNull()) {
            result->value_ = ValueFactory::GetIntegerValue(1);
          } else {
            result->value_ = result->value_.Add(ValueFactory::GetIntegerValue(1));
          }
        }
        break;
      case WindowFunctionType::SumAggregate:
        if (result->value_.IsNull()) {
          result->value_ = input.value_;
        } else if (!input.value_.IsNull()) {
          result->value_ = result->value_.Add(input.value_);
        }
        break;
      case WindowFunctionType::MinAggregate:
        if (result->value_.IsNull() || input.value_.CompareLessThan(result->value_) == CmpBool::CmpTrue) {
          result->value_ = input.value_;
        }
        break;
      case WindowFunctionType::MaxAggregate:
        if (result->value_.IsNull() || input.value_.CompareGreaterThan(result->value_) == CmpBool::CmpTrue) {
          result->value_ = input.value_;
        }
        break;
      case WindowFunctionType::Rank:
        result->value_ = result->value_.Add(ValueFactory::GetIntegerValue(1));
        break;
    }
  }

  /**
   * Inserts a value into the hash table and then combines it with the current aggregation.
   * @param agg_key the key to be inserted
   * @param agg_val the value to be inserted
   * @return the aggregation value after the insertion
   */
  auto InsertCombine(Tuple &tuple, const Schema &schema) -> WindowFunctionValue & {
    WindowFunctionKey key = GetWindowFunctionKey(tuple, schema);
    WindowFunctionValue val{function_->Evaluate(&tuple, schema)};

    if (ht_.count(key) == 0) {
      ht_.insert({key, GenerateInitialWindowFunctionValue()});
    }
    auto result = &ht_[key];
    CombineWindowFunctionValues(result, val);
    return *result;
  }

  void AddEmptyEntry() { ht_.insert({WindowFunctionKey(), GenerateInitialWindowFunctionValue()}); }

  /**
   * Clear the hash table
   */
  void Clear() { ht_.clear(); }

  /**
   * Get the result of the window function for the given tuple.
   */
  auto GetResult(Tuple &tuple, const Schema &schema) -> WindowFunctionValue & {
    return ht_[GetWindowFunctionKey(tuple, schema)];
  }

  auto GetType() -> WindowFunctionType { return function_type_; }

 private:
  /**
   * Get the partition by keys for the given tuple.
   */
  inline auto GetWindowFunctionKey(Tuple &tuple, const Schema &schema) -> WindowFunctionKey {
    WindowFunctionKey key;
    for (const auto &partition_by : partition_bys_) {
      key.keys_.emplace_back(partition_by->Evaluate(&tuple, schema));
    }
    return key;
  }

  /** The hash table is just a map from aggregate keys to aggregate values */
  std::unordered_map<WindowFunctionKey, WindowFunctionValue> ht_{};

  const std::vector<AbstractExpressionRef> &partition_bys_;

  const AbstractExpressionRef &function_;

  /** The types of aggregations that we have */
  const WindowFunctionType function_type_;
};

/**
 * Entry for sorting original tuples
 */
struct WindowSortEntry {
  /**
   * The keys for sorting, calculated from order by clauses
   */
  std::vector<Value> keys_;

  Tuple tuple_;
  RID rid_;
};

class WindowSortComparator {
  std::vector<OrderByType> order_by_types_;

 public:
  explicit WindowSortComparator(std::vector<OrderByType> order_by_types) : order_by_types_(std::move(order_by_types)) {}

  auto operator()(const WindowSortEntry &a, const WindowSortEntry &b) const -> bool {
    for (size_t i = 0; i < a.keys_.size(); i++) {
      switch (order_by_types_[i]) {
        case OrderByType::DEFAULT:
        case OrderByType::ASC:
          if (a.keys_[i].CompareLessThan(b.keys_[i]) == CmpBool::CmpTrue) {
            return true;
          }
          if (a.keys_[i].CompareGreaterThan(b.keys_[i]) == CmpBool::CmpTrue) {
            return false;
          }
          break;
        case OrderByType::DESC:
          // < of desc is considered as > of asc and vice versa
          if (a.keys_[i].CompareGreaterThan(b.keys_[i]) == CmpBool::CmpTrue) {
            return true;
          }
          if (a.keys_[i].CompareLessThan(b.keys_[i]) == CmpBool::CmpTrue) {
            return false;
          }
          break;
        default:
          throw std::runtime_error("Unknown OrderByType");
      }
    }
    return false;
  }
};

/**
 * The WindowFunctionExecutor executor executes a window function for columns using window function.
 *
 * Window function is different from normal aggregation as it outputs one row for each inputing rows,
 * and can be combined with normal selected columns. The columns in WindowFunctionPlanNode contains both
 * normal selected columns and placeholder columns for window functions.
 *
 * For example, if we have a query like:
 *    SELECT 0.1, 0.2, SUM(0.3) OVER (PARTITION BY 0.2 ORDER BY 0.3), SUM(0.4) OVER (PARTITION BY 0.1 ORDER BY 0.2,0.3)
 *      FROM table;
 *
 * The WindowFunctionPlanNode contains following structure:
 *    columns: std::vector<AbstractExpressionRef>{0.1, 0.2, 0.-1(placeholder), 0.-1(placeholder)}
 *    window_functions_: {
 *      3: {
 *        partition_by: std::vector<AbstractExpressionRef>{0.2}
 *        order_by: std::vector<AbstractExpressionRef>{0.3}
 *        functions: std::vector<AbstractExpressionRef>{0.3}
 *        window_func_type: WindowFunctionType::SumAggregate
 *      }
 *      4: {
 *        partition_by: std::vector<AbstractExpressionRef>{0.1}
 *        order_by: std::vector<AbstractExpressionRef>{0.2,0.3}
 *        functions: std::vector<AbstractExpressionRef>{0.4}
 *        window_func_type: WindowFunctionType::SumAggregate
 *      }
 *    }
 *
 * Your executor should use child executor and exprs in columns to produce selected columns except for window
 * function columns, and use window_agg_indexes, partition_bys, order_bys, functionss and window_agg_types to
 * generate window function columns results. Directly use placeholders for window function columns in columns is
 * not allowed, as it contains invalid column id.
 *
 * Your WindowFunctionExecutor does not need to support specified window frames (eg: 1 preceding and 1 following).
 * You can assume that all window frames are UNBOUNDED PRCEDING AND CURRENT ROW when there is ORDER BY clause, and
 * UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING when there is no ORDER BY clause.
 *
 */
class WindowFunctionExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new WindowFunctionExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The window aggregation plan to be executed
   */
  WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                         std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the window aggregation */
  void Init() override;

  /**
   * Yield the next tuple from the window aggregation.
   * @param[out] tuple The next tuple produced by the window aggregation
   * @param[out] rid The next tuple RID produced by the window aggregation
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the window aggregation plan */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  /**
   * Check if the current tuple has the same order by keys as the last entry.
   */
  auto IsEqualToLastEntry(const WindowSortEntry &entry) -> bool {
    if (last_entry_ == nullptr) {
      return false;
    }

    for (size_t i = 0; i < entry.keys_.size(); i++) {
      if (entry.keys_[i].CompareEquals(last_entry_->keys_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }

    return true;
  }

 private:
  /** The window aggregation plan node to be executed */
  const WindowFunctionPlanNode *plan_;

  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;

  /** The hash table for window function */
  std::unordered_map<uint32_t, std::unique_ptr<SimpleWindowHashTable>> window_hash_tables_;

  /** Sorted original tuples with order keys */
  std::vector<WindowSortEntry> window_sort_entries_;

  bool has_order_by_ = false;

  /** The iterator for sorted original tuples */
  std::vector<WindowSortEntry>::iterator window_sort_iter_;

  /** The original tuples without sorting */
  std::vector<Tuple> original_tuples_;
  std::vector<Tuple>::iterator original_tuple_iter_;

  /** The last entry */
  WindowSortEntry *last_entry_ = nullptr;
  /** The last rank values */
  std::unordered_map<uint32_t, Value> last_rank_values_;
};
}  // namespace bustub

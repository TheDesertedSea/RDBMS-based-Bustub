//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// sort_executor.h
//
// Identification: src/include/execution/executors/sort_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <vector>

#include "binder/bound_order_by.h"
#include "common/rid.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/sort_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

struct SortEntry {
  /** Keys calculated from order by clauses */
  std::vector<Value> keys_;

  Tuple tuple_;
  RID rid_;
};

class SortComparator {
  std::vector<OrderByType> order_by_types_;

 public:
  explicit SortComparator(std::vector<OrderByType> order_by_types) : order_by_types_(std::move(order_by_types)) {}

  auto operator()(const SortEntry &a, const SortEntry &b) const -> bool {
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
 * The SortExecutor executor executes a sort.
 */
class SortExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new SortExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The sort plan to be executed
   */
  SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the sort */
  void Init() override;

  /**
   * Yield the next tuple from the sort.
   * @param[out] tuple The next tuple produced by the sort
   * @param[out] rid The next tuple RID produced by the sort
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the sort */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  /** The sort plan node to be executed */
  const SortPlanNode *plan_;

  /** The child executor whose output is to be sorted */
  std::unique_ptr<AbstractExecutor> child_executor_;

  /** The sorted entries */
  std::vector<SortEntry> entries_;

  std::vector<SortEntry>::iterator entry_itr_;
};
}  // namespace bustub

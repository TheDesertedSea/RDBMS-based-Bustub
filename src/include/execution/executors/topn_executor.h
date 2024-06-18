//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// topn_executor.h
//
// Identification: src/include/execution/executors/topn_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <queue>
#include <utility>
#include <vector>

#include "binder/bound_order_by.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/topn_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

struct TopNEntry {
  std::vector<Value> keys_;
  std::vector<OrderByType> order_by_types_;

  Tuple tuple_;
  RID rid_;
};

class TopNComparator {
 public:
  auto operator()(const TopNEntry &a, const TopNEntry &b) const -> bool {
    for (size_t i = 0; i < a.keys_.size(); i++) {
      switch (a.order_by_types_[i]) {
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
 * The TopNExecutor executor executes a topn.
 */
class TopNExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new TopNExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The TopN plan to be executed
   */
  TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the TopN */
  void Init() override;

  /**
   * Yield the next tuple from the TopN.
   * @param[out] tuple The next tuple produced by the TopN
   * @param[out] rid The next tuple RID produced by the TopN
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the TopN */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  /** Sets new child executor (for testing only) */
  void SetChildExecutor(std::unique_ptr<AbstractExecutor> &&child_executor) {
    child_executor_ = std::move(child_executor);
  }

  /** @return The size of top_entries_ container, which will be called on each child_executor->Next(). */
  auto GetNumInHeap() -> size_t;

 private:
  /** The TopN plan node to be executed */
  const TopNPlanNode *plan_;
  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;

  /** Heap for maintaining top n entries */
  std::unique_ptr<std::priority_queue<TopNEntry, std::vector<TopNEntry>, TopNComparator>> top_entries_;

  /** Reversed entries from heap since heap is max heap*/
  std::vector<TopNEntry> sorted_top_entries_;

  std::vector<TopNEntry>::iterator entry_itr_;
};
}  // namespace bustub

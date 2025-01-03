//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.h
//
// Identification: src/include/execution/executors/update_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "concurrency/transaction_manager.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/update_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * UpdateExecutor executes an update on a table.
 * Updated values are always pulled from a child.
 */
class UpdateExecutor : public AbstractExecutor {
  friend class UpdatePlanNode;

 public:
  /**
   * Construct a new UpdateExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The update plan to be executed
   * @param child_executor The child executor that feeds the update
   */
  UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                 std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the update */
  void Init() override;

  /**
   * Yield the next tuple from the update.
   * @param[out] tuple The next tuple produced by the update
   * @param[out] rid The next tuple RID produced by the update (ignore this)
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   *
   * NOTE: UpdateExecutor::Next() does not use the `rid` out-parameter.
   */
  auto Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the update */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  /**
   * Update the tuple in place(at the same RID)
   *
   * @param r the RID of the tuple
   * @param m the tuple meta
   * @param old_tuple the old tuple
   * @param t the new tuple
   * @param version_link the version link
   */
  void UpdateInPlace(RID r, const Tuple &t);

  void UpdateInPlaceUsingLock(RID r, const Tuple &t);

  /**
   * Insert a new tuple into the table
   * This is for updated tuple that has changed primary key
   *
   * @param t the tuple to be inserted
   */
  void InsertNewTuple(Tuple &t);
  /**
   * Delete the old tuple from the table
   *
   * @param r the RID of the tuple
   * @param m the tuple meta
   * @param old_tuple the old tuple
   * @param version_link the version link
   */
  void DeleteOldTuple(RID r);

  void DeleteOldTupleUsingLock(RID r);

  /**
   * Insert a tuple at the given RID, which is pointed by an existing index
   * This is like an update operation.
   *
   * @param r The RID to be inserted at
   * @param t The tuple to be inserted
   */
  void InsertWithExistingIndex(RID r, const Tuple &t);

  void InsertWithExistingIndexUsingLock(RID r, const Tuple &t);

  /**
   * Create a new tuple and insert it into the table.
   * Also create new indexes for the tuple.
   *
   * @param t The tuple to be inserted
   * @param new_rid [out] The RID of the inserted tuple
   */
  void InsertWithNewIndex(Tuple &t, RID *new_rid);

  // Update tuple on the table heap
  inline void UpdateTuple(const Tuple &t, const RID &r) {
    TupleMeta m{txn_->GetTransactionTempTs(), false};
    table_info_->table_->UpdateTupleInPlace(m, t, r);
  }

  inline void UpdateTupleWithLocking(const Tuple &t, const RID &r, TablePage *page) {
    TupleMeta m{txn_->GetTransactionTempTs(), false};
    table_info_->table_->UpdateTupleInPlaceWithLockAcquired(m, t, r, page);
  }

  // Mark a tuple as deleted on the table heap
  inline void DeleteTuple(const RID &r) {
    TupleMeta m{txn_->GetTransactionTempTs(), true};
    table_info_->table_->UpdateTupleMeta(m, r);
  }

  inline void DeleteTupleWithLocking(const RID &r, const Tuple &t, TablePage *page) {
    TupleMeta m{txn_->GetTransactionTempTs(), true};
    table_info_->table_->UpdateTupleInPlaceWithLockAcquired(m, t, r, page);
  }

  /** The update plan node to be executed */
  const UpdatePlanNode *plan_;

  /** The child executor to obtain value from */
  std::unique_ptr<AbstractExecutor> child_executor_;

  bool updated_{false};

  TableInfo *table_info_;
  Transaction *txn_;
  std::vector<IndexInfo *> indexes_;
};
}  // namespace bustub

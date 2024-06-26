//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include "catalog/schema.h"
#include "common/config.h"
#include "common/exception.h"
#include "concurrency/transaction.h"
#include "execution/execution_common.h"
#include "storage/table/tuple.h"

#include "concurrency/transaction_manager.h"
#include "execution/executors/update_executor.h"
#include "type/type.h"
#include "type/value_factory.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() {
  child_executor_->Init();
  updated_ = false;
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (updated_) {
    return false;
  }

  auto catalog = exec_ctx_->GetCatalog();
  auto table_info = catalog->GetTable(plan_->GetTableOid());
  auto indexes = catalog->GetTableIndexes(table_info->name_);
  auto txn = exec_ctx_->GetTransaction();
  auto txn_mgr = exec_ctx_->GetTransactionManager();

  // Get all the tuples to update
  std::vector<std::tuple<Tuple, RID, TupleMeta>> tuples_to_update;
  Tuple t;
  RID r;
  while (child_executor_->Next(&t, &r)) {
    // check write-write conflict
    auto tuple_meta = table_info->table_->GetTupleMeta(r);
    if (tuple_meta.ts_ > txn->GetReadTs() && tuple_meta.ts_ != txn->GetTransactionTempTs()) {
      txn->SetTainted();
      throw ExecutionException("Write-write conflict detected in UpdateExecutor");
    }

    tuples_to_update.emplace_back(t, r, tuple_meta);
  }

  int count_updated = 0;
  // update each tuple
  for (auto &[t, r, meta] : tuples_to_update) {
    // generate the new tuple
    std::vector<Value> values;
    for (auto const &target_expr : plan_->target_expressions_) {
      values.push_back(target_expr->Evaluate(&t, child_executor_->GetOutputSchema()));
    }
    Tuple new_tuple(values, &child_executor_->GetOutputSchema());

    if (IsTupleContentEqual(t, new_tuple)) {
      continue;
    }

    // update the undo log
    auto first_undo_link = txn_mgr->GetUndoLink(r);
    if (first_undo_link.has_value() && first_undo_link->IsValid()) {
      // has undo log
      if (first_undo_link->prev_txn_ == txn->GetTransactionId()) {
        // first undo log is created by this transaction, reuse
        auto first_undo_log = txn_mgr->GetUndoLog(first_undo_link.value());
        if (!first_undo_log.is_deleted_) {
          std::vector<bool> merged_modified_fields(table_info->schema_.GetColumnCount(), false);
          first_undo_log.tuple_ = MergeParitalTuple(table_info->schema_, t, values, first_undo_log.tuple_,
                                                    first_undo_log.modified_fields_, merged_modified_fields);
          first_undo_log.modified_fields_ = merged_modified_fields;
        }
        txn->ModifyUndoLog(first_undo_link->prev_log_idx_, first_undo_log);
      } else {
        // first undo log is not created by this transaction, create a new undo log
        auto undo_log = UndoLog();
        undo_log.is_deleted_ = meta.is_deleted_;  // original tuple may be deleted, some transaction may delete it and
                                                  // commit between the child executor and this executor
        undo_log.modified_fields_ = std::vector<bool>(table_info->schema_.GetColumnCount(), false);
        undo_log.tuple_ = meta.is_deleted_
                              ? Tuple()
                              : GeneratePartialTuple(table_info->schema_, t, values, undo_log.modified_fields_);
        undo_log.prev_version_ = first_undo_link.value();
        undo_log.ts_ = meta.ts_;

        txn_mgr->UpdateUndoLink(r, txn->AppendUndoLog(undo_log));
      }
    } else if (meta.ts_ != txn->GetTransactionTempTs()) {
      // no undo log and this tuple is not created by this transaction, create a new undo log
      auto undo_log = UndoLog();
      undo_log.is_deleted_ = meta.is_deleted_;  // original tuple may be deleted
      undo_log.modified_fields_ = std::vector<bool>(table_info->schema_.GetColumnCount(), false);
      undo_log.tuple_ =
          meta.is_deleted_ ? Tuple() : GeneratePartialTuple(table_info->schema_, t, values, undo_log.modified_fields_);
      undo_log.ts_ = meta.ts_;

      txn_mgr->UpdateUndoLink(r, txn->AppendUndoLog(undo_log));
    }

    // update the tuple
    TupleMeta new_meta;
    new_meta.is_deleted_ = false;
    new_meta.ts_ = txn->GetTransactionTempTs();
    table_info->table_->UpdateTupleInPlace(new_meta, new_tuple, r);

    for (auto &index_info : indexes) {
      // Delete the old index entry then insert the new index entry
      index_info->index_->DeleteEntry(
          t.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()), r,
          exec_ctx_->GetTransaction());

      auto result = index_info->index_->InsertEntry(
          new_tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()), r,
          exec_ctx_->GetTransaction());
      if (!result) {
        txn->SetTainted();
        throw ExecutionException("Insert index entry failed in UpdateExecutor");
      }
    }

    txn->AppendWriteSet(plan_->GetTableOid(), r);  // append to write set
    count_updated++;
  }
  *tuple = Tuple{{ValueFactory::GetIntegerValue(count_updated)}, &GetOutputSchema()};

  updated_ = true;
  return true;
}

}  // namespace bustub

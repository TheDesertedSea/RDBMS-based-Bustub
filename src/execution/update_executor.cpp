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
#include <optional>
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
  auto catalog = exec_ctx_->GetCatalog();
  table_info_ = catalog->GetTable(plan_->GetTableOid());
  indexes_ = catalog->GetTableIndexes(table_info_->name_);
  txn_ = exec_ctx_->GetTransaction();
  updated_ = false;
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (updated_) {
    return false;
  }

  auto txn_mgr = exec_ctx_->GetTransactionManager();
  // find out primary index
  IndexInfo *primary_index = nullptr;
  for (auto index_info : indexes_) {
    if (index_info->is_primary_key_) {
      primary_index = index_info;
      break;
    }
  }

  // Get all tuples to update and all tuples to delete and insert
  std::vector<std::tuple<Tuple, RID>> tuples_to_update;
  std::vector<std::tuple<Tuple, RID>> tuples_to_delete_insert;
  Tuple t;
  RID r;
  while (child_executor_->Next(&t, &r)) {
    // generate the new tuple
    std::vector<Value> values;
    for (auto const &target_expr : plan_->target_expressions_) {
      values.push_back(target_expr->Evaluate(&t, child_executor_->GetOutputSchema()));
    }
    Tuple new_tuple(values, &child_executor_->GetOutputSchema());

    if (primary_index != nullptr) {
      auto prev_key =
          t.KeyFromTuple(table_info_->schema_, primary_index->key_schema_, primary_index->index_->GetKeyAttrs());
      auto new_key = new_tuple.KeyFromTuple(table_info_->schema_, primary_index->key_schema_,
                                            primary_index->index_->GetKeyAttrs());
      if (!IsTupleContentEqual(prev_key, new_key)) {
        // primary key has been changed, need insert new tuple and new index entry(if new index not exists)
        tuples_to_delete_insert.emplace_back(new_tuple, r);
        continue;
      }
    }

    tuples_to_update.emplace_back(new_tuple, r);
  }

  // delete old tuple first to get empty slot, we cannot move a tuple to a existing tuple. If that happens after
  // deleting all old tuples, it's a write-write conflict.
  for (auto &[new_tuple, r] : tuples_to_delete_insert) {
    auto version_link = txn_mgr->GetVersionLink(r);
    CheckAndLockVersionLink(r, txn_, txn_mgr, &version_link, table_info_);

    auto [meta, old_tuple] = table_info_->table_->GetTuple(r);

    TupleMeta m = table_info_->table_->GetTupleMeta(r);
    DeleteOldTuple(r, m, old_tuple, version_link);
  }

  int count_updated = 0;
  // insert new tuple
  for (auto &[new_tuple, r] : tuples_to_delete_insert) {
    InsertNewTuple(new_tuple);
    count_updated++;
  }

  // update each tuple that needs to be updated
  for (auto &[new_tuple, r] : tuples_to_update) {
    auto version_link = txn_mgr->GetVersionLink(r);
    CheckAndLockVersionLink(r, txn_, txn_mgr, &version_link, table_info_);

    auto [meta, old_tuple] = table_info_->table_->GetTuple(r);
    UpdateInPlace(r, meta, old_tuple, new_tuple, version_link);
    count_updated++;
  }

  *tuple = Tuple{{ValueFactory::GetIntegerValue(count_updated)}, &GetOutputSchema()};
  updated_ = true;
  return true;
}

void UpdateExecutor::UpdateInPlace(RID r, TupleMeta &m, const Tuple &old_tuple, const Tuple &t,
                                   std::optional<VersionUndoLink> &version_link) {
  auto txn_mgr = exec_ctx_->GetTransactionManager();

  if (version_link.has_value() && version_link->prev_.IsValid()) {
    // has undo log
    if (version_link->prev_.prev_txn_ == txn_->GetTransactionId()) {
      // first undo log is created by this transaction, reuse
      auto first_undo_log = txn_mgr->GetUndoLog(version_link->prev_);
      if (!first_undo_log.is_deleted_) {
        std::vector<bool> merged_modified_fields(table_info_->schema_.GetColumnCount(), false);
        first_undo_log.tuple_ = MergeParitalTuple(table_info_->schema_, old_tuple, t, first_undo_log.tuple_,
                                                  first_undo_log.modified_fields_, merged_modified_fields);
        first_undo_log.modified_fields_ = merged_modified_fields;
      }
      txn_->ModifyUndoLog(version_link->prev_.prev_log_idx_, first_undo_log);

      // create a tuple on the table heap with a transaction temporary timestamp
      UpdateTuple(t, r);
      version_link->in_progress_ = false;
      txn_mgr->UpdateVersionLink(r, version_link);
    } else {
      // first undo log is not created by this transaction, create a new undo log
      auto undo_log = UndoLog();
      undo_log.is_deleted_ = m.is_deleted_;  // original tuple may be deleted, some transaction may delete it and
                                             // commit between the child executor and this executor
      undo_log.modified_fields_ = std::vector<bool>(table_info_->schema_.GetColumnCount(), false);
      undo_log.tuple_ =
          m.is_deleted_ ? Tuple() : GeneratePartialTuple(table_info_->schema_, old_tuple, t, undo_log.modified_fields_);
      undo_log.prev_version_ = version_link->prev_;
      undo_log.ts_ = m.ts_;

      // need to update the version link before updating the tuple on the table heap
      auto new_version_link = VersionUndoLink::FromOptionalUndoLink(txn_->AppendUndoLog(undo_log));
      new_version_link->in_progress_ = true;
      txn_mgr->UpdateVersionLink(r, new_version_link);
      // update the tuple on the table heap
      UpdateTuple(t, r);
      // set the in_progress_ flag to false
      new_version_link->in_progress_ = false;
      txn_mgr->UpdateVersionLink(r, new_version_link);
    }
  } else if (m.ts_ != txn_->GetTransactionTempTs()) {
    // no undo log and this tuple is not created by this transaction, create a new undo log
    auto undo_log = UndoLog();
    undo_log.is_deleted_ = m.is_deleted_;  // original tuple may be deleted
    undo_log.modified_fields_ = std::vector<bool>(table_info_->schema_.GetColumnCount(), false);
    undo_log.tuple_ =
        m.is_deleted_ ? Tuple() : GeneratePartialTuple(table_info_->schema_, old_tuple, t, undo_log.modified_fields_);
    undo_log.ts_ = m.ts_;

    // need to update the version link before updating the tuple on the table heap
    auto new_version_link = VersionUndoLink::FromOptionalUndoLink(txn_->AppendUndoLog(undo_log));
    new_version_link->in_progress_ = true;
    txn_mgr->UpdateVersionLink(r, new_version_link);
    // update the tuple on the table heap
    UpdateTuple(t, r);
    // set the in_progress_ flag to false
    new_version_link->in_progress_ = false;
    txn_mgr->UpdateVersionLink(r, new_version_link);
  } else {
    // no version link and this tuple is created by this transaction
    UpdateTuple(t, r);
    version_link->in_progress_ = false;
    txn_mgr->UpdateVersionLink(r, version_link);
  }

  txn_->AppendWriteSet(table_info_->oid_, r);
}

void UpdateExecutor::DeleteOldTuple(RID r, TupleMeta &m, const Tuple &old_tuple,
                                    std::optional<VersionUndoLink> &version_link) {
  auto txn_mgr = exec_ctx_->GetTransactionManager();

  // update undo log
  if (version_link.has_value() && version_link->prev_.IsValid()) {
    // has undo log
    if (version_link->prev_.prev_txn_ == txn_->GetTransactionId()) {
      // first undo log is created by this transaction, reuse
      auto first_undo_log = txn_mgr->GetUndoLog(version_link->prev_);
      if (!first_undo_log.is_deleted_) {
        auto new_partial_tuple = ReconstructTuple(&table_info_->schema_, old_tuple, m, {first_undo_log});
        first_undo_log.modified_fields_ = std::vector<bool>(table_info_->schema_.GetColumnCount(), true);
        BUSTUB_ASSERT(new_partial_tuple.has_value(), "ReconstructTuple should return a tuple here");
        first_undo_log.tuple_ = new_partial_tuple.value();
      }
      txn_->ModifyUndoLog(version_link->prev_.prev_log_idx_, first_undo_log);

      // delete the tuple
      DeleteTuple(r);
      version_link->in_progress_ = false;
      txn_mgr->UpdateVersionLink(r, version_link);
    } else {
      // first undo log is not created by this transaction, create a new undo log
      UndoLog undo_log;
      undo_log.is_deleted_ = false;  // should not be deleted, or it should already be "continued" before
      undo_log.modified_fields_ = std::vector<bool>(table_info_->schema_.GetColumnCount(), true);
      undo_log.tuple_ = old_tuple;
      undo_log.ts_ = m.ts_;
      undo_log.prev_version_ = version_link->prev_;  // link to the previous version

      // need to update the version link before mark the tuple as deleted on the table heap
      auto new_version_link = VersionUndoLink::FromOptionalUndoLink(txn_->AppendUndoLog(undo_log));
      new_version_link->in_progress_ = true;
      txn_mgr->UpdateVersionLink(r, new_version_link);
      // delete the tuple on the table heap
      DeleteTuple(r);
      // set the in_progress_ flag to false
      new_version_link->in_progress_ = false;
      txn_mgr->UpdateVersionLink(r, new_version_link);
    }
  } else if (m.ts_ != txn_->GetTransactionTempTs()) {
    // no undo log and this tuple is not created by this transaction, create a new undo log
    UndoLog undo_log;
    undo_log.is_deleted_ = false;  // should not be deleted, or it should already be "continued" before
    undo_log.modified_fields_ = std::vector<bool>(table_info_->schema_.GetColumnCount(), true);
    undo_log.tuple_ = old_tuple;
    undo_log.ts_ = m.ts_;

    // need to update the version link before mark the tuple as deleted on the table heap
    auto new_version_link = VersionUndoLink::FromOptionalUndoLink(txn_->AppendUndoLog(undo_log));
    new_version_link->in_progress_ = true;
    txn_mgr->UpdateVersionLink(r, new_version_link);
    // delete the tuple on the table heap
    DeleteTuple(r);
    // set the in_progress_ flag to false
    new_version_link->in_progress_ = false;
    txn_mgr->UpdateVersionLink(r, new_version_link);
  } else {
    // no version link and this tuple is created by this transaction
    DeleteTuple(r);
    version_link->in_progress_ = false;
    txn_mgr->UpdateVersionLink(r, version_link);
  }

  txn_->AppendWriteSet(plan_->GetTableOid(), r);  // append to write set
}

void UpdateExecutor::InsertNewTuple(Tuple &t) {
  auto has_index = false;
  for (auto &index_info : indexes_) {
    std::vector<RID> result;
    index_info->index_->ScanKey(
        t.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()), &result,
        txn_);
    if (!result.empty()) {
      // need to delete the old tuple and insert the new tuple
      InsertWithExistingIndex(result[0], t);
      has_index = true;
      break;
    }
  }

  if (!has_index) {
    RID new_rid;
    InsertWithNewIndex(t, &new_rid);
  }
}

void UpdateExecutor::InsertWithExistingIndex(const RID r, const Tuple &t) {
  auto txn_mgr = exec_ctx_->GetTransactionManager();
  auto version_link = txn_mgr->GetVersionLink(r);
  CheckAndLockVersionLink(r, txn_, txn_mgr, &version_link, table_info_);

  auto [m, old_tuple] = table_info_->table_->GetTuple(r);

  if (!m.is_deleted_) {
    // this is write-write conflict, we cannot update a tuple that should not be updated
    version_link->in_progress_ = false;
    txn_mgr->UpdateVersionLink(r, version_link);
    txn_->SetTainted();
    throw ExecutionException("InsertWithExistingIndex: Write-write conflict, aborting");
  }

  if (version_link.has_value() && version_link->prev_.IsValid()) {
    // has undo log
    if (version_link->prev_.prev_txn_ == txn_->GetTransactionId()) {
      // first undo log is created by this transaction, reuse
      auto first_undo_log = txn_mgr->GetUndoLog(version_link->prev_);
      if (!first_undo_log.is_deleted_) {
        std::vector<bool> merged_modified_fields(table_info_->schema_.GetColumnCount(), false);
        first_undo_log.tuple_ = MergeParitalTuple(table_info_->schema_, old_tuple, t, first_undo_log.tuple_,
                                                  first_undo_log.modified_fields_, merged_modified_fields);
        first_undo_log.modified_fields_ = merged_modified_fields;
      }
      txn_->ModifyUndoLog(version_link->prev_.prev_log_idx_, first_undo_log);

      // create a tuple on the table heap with a transaction temporary timestamp
      UpdateTuple(t, r);
      version_link->in_progress_ = false;
      txn_mgr->UpdateVersionLink(r, version_link);
    } else {
      // first undo log is not created by this transaction, create a new undo log
      auto undo_log = UndoLog();
      undo_log.is_deleted_ = m.is_deleted_;  // original tuple may be deleted, some transaction may delete it and
                                             // commit between the child executor and this executor
      undo_log.modified_fields_ = std::vector<bool>(table_info_->schema_.GetColumnCount(), false);
      undo_log.tuple_ =
          m.is_deleted_ ? Tuple() : GeneratePartialTuple(table_info_->schema_, old_tuple, t, undo_log.modified_fields_);
      undo_log.prev_version_ = version_link->prev_;
      undo_log.ts_ = m.ts_;

      // need to update the version link before updating the tuple on the table heap
      auto new_version_link = VersionUndoLink::FromOptionalUndoLink(txn_->AppendUndoLog(undo_log));
      new_version_link->in_progress_ = true;
      txn_mgr->UpdateVersionLink(r, new_version_link);
      // update the tuple on the table heap
      UpdateTuple(t, r);
      // set the in_progress_ flag to false
      new_version_link->in_progress_ = false;
      txn_mgr->UpdateVersionLink(r, new_version_link);
    }
  } else if (m.ts_ != txn_->GetTransactionTempTs()) {
    // no undo log and this tuple is not created by this transaction, create a new undo log
    auto undo_log = UndoLog();
    undo_log.is_deleted_ = m.is_deleted_;  // original tuple may be deleted
    undo_log.modified_fields_ = std::vector<bool>(table_info_->schema_.GetColumnCount(), false);
    undo_log.tuple_ =
        m.is_deleted_ ? Tuple() : GeneratePartialTuple(table_info_->schema_, old_tuple, t, undo_log.modified_fields_);
    undo_log.ts_ = m.ts_;

    // need to update the version link before updating the tuple on the table heap
    auto new_version_link = VersionUndoLink::FromOptionalUndoLink(txn_->AppendUndoLog(undo_log));
    new_version_link->in_progress_ = true;
    txn_mgr->UpdateVersionLink(r, new_version_link);
    // update the tuple on the table heap
    UpdateTuple(t, r);
    // set the in_progress_ flag to false
    new_version_link->in_progress_ = false;
    txn_mgr->UpdateVersionLink(r, new_version_link);
  } else {
    // no version link and this tuple is created by this transaction
    UpdateTuple(t, r);
    version_link->in_progress_ = false;
    txn_mgr->UpdateVersionLink(r, version_link);
  }

  txn_->AppendWriteSet(table_info_->oid_, r);
}

void UpdateExecutor::InsertWithNewIndex(Tuple &t, RID *new_rid) {
  // create a tuple on the table heap with a transaction temporary timestamp
  TupleMeta meta;
  meta.is_deleted_ = false;
  meta.ts_ = txn_->GetTransactionTempTs();
  auto result = table_info_->table_->InsertTuple(meta, t, exec_ctx_->GetLockManager(), txn_, plan_->GetTableOid());
  BUSTUB_ASSERT(result.has_value(), "InsertTuple should return a RID on success");
  *new_rid = result.value();
  txn_->AppendWriteSet(table_info_->oid_, *new_rid);

  // insert the tuple into the index
  for (auto &index_info : indexes_) {
    auto result = index_info->index_->InsertEntry(
        t.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()), *new_rid,
        exec_ctx_->GetTransaction());
    if (!result) {
      // Another concurrent transaction is also inserting the same tuple, abort (We only has primary key index)
      txn_->SetTainted();
      throw ExecutionException("InsertWithNewIndex: Another transaction has inserted the same key, aborting");
    }
  }
}

}  // namespace bustub

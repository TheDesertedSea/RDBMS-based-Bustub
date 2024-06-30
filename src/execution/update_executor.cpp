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

  // Get all the tuples to update
  std::vector<std::tuple<Tuple, RID, std::optional<VersionUndoLink>>> tuples_to_update;
  std::vector<std::tuple<Tuple, RID, std::optional<VersionUndoLink>>> tuples_to_delete_insert;
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
        // need insert new tuple and new index entry(if new index not exists)
        tuples_to_delete_insert.emplace_back(new_tuple, r, txn_mgr->GetVersionLink(r));
        continue;
      }
    }

    tuples_to_update.emplace_back(new_tuple, r, txn_mgr->GetVersionLink(r));
  }

  // delete old tuple first
  for (auto &[new_tuple, r, version_link] : tuples_to_delete_insert) {
    LockVersionLink(r, txn_, txn_mgr, &version_link);

    // check write-write conflict
    auto [meta, old_tuple] = table_info_->table_->GetTuple(r);

    if (meta.ts_ > txn_->GetReadTs() && meta.ts_ != txn_->GetTransactionTempTs()) {
      version_link->in_progress_ = false;
      txn_mgr->UpdateVersionLink(r, version_link);
      txn_->SetTainted();
      std::cout << "Write-write conflict in UpdateExecutor 1" << std::endl;
      throw ExecutionException("Write-write conflict in UpdateExecutor 1");
    }

    TupleMeta m = table_info_->table_->GetTupleMeta(r);
    DeleteOldTuple(r, m, old_tuple, version_link);
  }

  int count_updated = 0;
  // insert new tuple
  for (auto &[new_tuple, r, version_link] : tuples_to_delete_insert) {
    InsertNewTuple(new_tuple);
    count_updated++;
  }

  // update each tuple that needs to be updated
  for (auto &[new_tuple, r, version_link] : tuples_to_update) {
    LockVersionLink(r, txn_, txn_mgr, &version_link);
    // check write-write conflict
    auto [meta, old_tuple] = table_info_->table_->GetTuple(r);
    if (IsTupleContentEqual(old_tuple, new_tuple)) {
      version_link->in_progress_ = false;
      txn_mgr->UpdateVersionLink(r, version_link);
      continue;
    }

    if (meta.ts_ > txn_->GetReadTs() && meta.ts_ != txn_->GetTransactionTempTs()) {
      version_link->in_progress_ = false;
      txn_mgr->UpdateVersionLink(r, version_link);
      txn_->SetTainted();
      std::cout << "Write-write conflict in UpdateExecutor 2" << std::endl;
      throw ExecutionException("Write-write conflict in UpdateExecutor 2");
    }

    UpdateInPlace(r, meta, old_tuple, new_tuple, version_link);
    count_updated++;
  }

  *tuple = Tuple{{ValueFactory::GetIntegerValue(count_updated)}, &GetOutputSchema()};
  updated_ = true;
  return true;
}

void UpdateExecutor::UpdateInPlace(RID r, TupleMeta &m, const Tuple &old_tuple, const Tuple &t,
                                   std::optional<VersionUndoLink> &version_link) {
  // step 1, mark version_link->in_progress_ as true(lock)
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

      // step 2 create a tuple on the table heap with a transaction temporary timestamp
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

      // step 2 create a tuple on the table heap with a transaction temporary timestamp
      UpdateTuple(t, r);
      txn_mgr->UpdateVersionLink(r, VersionUndoLink::FromOptionalUndoLink(txn_->AppendUndoLog(undo_log)));
    }
  } else if (m.ts_ != txn_->GetTransactionTempTs()) {
    // no undo log and this tuple is not created by this transaction, create a new undo log
    auto undo_log = UndoLog();
    undo_log.is_deleted_ = m.is_deleted_;  // original tuple may be deleted
    undo_log.modified_fields_ = std::vector<bool>(table_info_->schema_.GetColumnCount(), false);
    undo_log.tuple_ =
        m.is_deleted_ ? Tuple() : GeneratePartialTuple(table_info_->schema_, old_tuple, t, undo_log.modified_fields_);
    undo_log.ts_ = m.ts_;

    // step 2 create a tuple on the table heap with a transaction temporary timestamp
    UpdateTuple(t, r);
    txn_mgr->UpdateVersionLink(r, VersionUndoLink::FromOptionalUndoLink(txn_->AppendUndoLog(undo_log)));
  } else {
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

      // delete the tuple
      DeleteTuple(r);
      txn_mgr->UpdateVersionLink(r, VersionUndoLink::FromOptionalUndoLink(txn_->AppendUndoLog(undo_log)));
    }
  } else if (m.ts_ != txn_->GetTransactionTempTs()) {
    // no undo log and this tuple is not created by this transaction, create a new undo log
    UndoLog undo_log;
    undo_log.is_deleted_ = false;  // should not be deleted, or it should already be "continued" before
    undo_log.modified_fields_ = std::vector<bool>(table_info_->schema_.GetColumnCount(), true);
    undo_log.tuple_ = old_tuple;
    undo_log.ts_ = m.ts_;

    // delete the tuple
    DeleteTuple(r);
    txn_mgr->UpdateVersionLink(r, VersionUndoLink::FromOptionalUndoLink(txn_->AppendUndoLog(undo_log)));
  } else {
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
  // step 1, mark version_link->in_progress_ as true(lock)
  auto txn_mgr = exec_ctx_->GetTransactionManager();
  auto version_link = txn_mgr->GetVersionLink(r);
  LockVersionLink(r, txn_, txn_mgr, &version_link);

  // check write-write conflict
  auto [m, old_tuple] = table_info_->table_->GetTuple(r);
  if (m.ts_ > txn_->GetReadTs() && m.ts_ != txn_->GetTransactionTempTs()) {
    version_link->in_progress_ = false;
    txn_mgr->UpdateVersionLink(r, version_link);
    txn_->SetTainted();
    std::cout << "Write-write conflict detected in UpdateExecutor(InsertWithExistingIndex) 1" << std::endl;
    throw ExecutionException("Write-write conflict detected in UpdateExecutor(InsertWithExistingIndex) 1");
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

      // step 2 create a tuple on the table heap with a transaction temporary timestamp
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

      // step 2 create a tuple on the table heap with a transaction temporary timestamp
      UpdateTuple(t, r);
      txn_mgr->UpdateVersionLink(r, VersionUndoLink::FromOptionalUndoLink(txn_->AppendUndoLog(undo_log)));
    }
  } else if (m.ts_ != txn_->GetTransactionTempTs()) {
    // no undo log and this tuple is not created by this transaction, create a new undo log
    auto undo_log = UndoLog();
    undo_log.is_deleted_ = m.is_deleted_;  // original tuple may be deleted
    undo_log.modified_fields_ = std::vector<bool>(table_info_->schema_.GetColumnCount(), false);
    undo_log.tuple_ =
        m.is_deleted_ ? Tuple() : GeneratePartialTuple(table_info_->schema_, old_tuple, t, undo_log.modified_fields_);
    undo_log.ts_ = m.ts_;

    // step 2 create a tuple on the table heap with a transaction temporary timestamp
    UpdateTuple(t, r);
    txn_mgr->UpdateVersionLink(r, VersionUndoLink::FromOptionalUndoLink(txn_->AppendUndoLog(undo_log)));
  } else {
    UpdateTuple(t, r);
    version_link->in_progress_ = false;
    txn_mgr->UpdateVersionLink(r, version_link);
  }

  txn_->AppendWriteSet(table_info_->oid_, r);
}

void UpdateExecutor::InsertWithNewIndex(Tuple &t, RID *new_rid) {
  // step 2 create a tuple on the table heap with a transaction temporary timestamp
  TupleMeta meta;
  meta.is_deleted_ = false;
  meta.ts_ = txn_->GetTransactionTempTs();
  auto result = table_info_->table_->InsertTuple(meta, t, exec_ctx_->GetLockManager(), txn_, plan_->GetTableOid());
  BUSTUB_ASSERT(result.has_value(), "InsertTuple should return a RID on success");
  *new_rid = result.value();
  txn_->AppendWriteSet(table_info_->oid_, *new_rid);

  // step 3 insert the tuple into the index
  for (auto &index_info : indexes_) {
    auto result = index_info->index_->InsertEntry(
        t.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()), *new_rid,
        exec_ctx_->GetTransaction());
    if (!result) {
      txn_->SetTainted();
      std::cout << "InsertWithNewIndex(UpdateExecutor): Another transaction has inserted the same key, aborting"
                << std::endl;
      throw ExecutionException("InsertWithNewIndex: Another transaction has inserted the same key, aborting");
    }
  }
}

}  // namespace bustub

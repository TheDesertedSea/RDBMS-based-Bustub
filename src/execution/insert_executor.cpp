//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include "catalog/schema.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "storage/table/tuple.h"
#include "type/integer_type.h"
#include "type/type.h"
#include "type/type_id.h"
#include "type/value_factory.h"

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  auto catalog = exec_ctx_->GetCatalog();
  table_info_ = catalog->GetTable(plan_->GetTableOid());
  indexes_ = catalog->GetTableIndexes(table_info_->name_);
  txn_ = exec_ctx_->GetTransaction();
  inserted_ = false;
  child_executor_->Init();
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (inserted_) {
    return false;
  }

  Tuple t;
  RID r;
  int count_inserted = 0;
  while (child_executor_->Next(&t, &r)) {
    // step 1 check if the tuple already exists in the index
    auto has_index = false;
    for (auto &index_info : indexes_) {
      std::vector<RID> result;
      index_info->index_->ScanKey(
          t.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()), &result,
          txn_);
      if (!result.empty()) {
        // InsertWithExistingIndexLock(result[0], t);
        InsertWithExistingIndexUsingLock(result[0], t);
        has_index = true;
        break;
      }
    }

    if (!has_index) {
      RID new_rid;
      InsertWithNewIndex(t, &new_rid);
    }
    count_inserted++;
  }
  *tuple = Tuple{{ValueFactory::GetIntegerValue(count_inserted)}, &GetOutputSchema()};

  inserted_ = true;
  return true;
}

void InsertExecutor::InsertWithExistingIndex(const RID r, const Tuple &t) {
  auto txn_mgr = exec_ctx_->GetTransactionManager();
  auto version_link = txn_mgr->GetVersionLink(r);
  CheckAndLockVersionLink(r, txn_, txn_mgr, &version_link, table_info_);

  auto [m, old_tuple] = table_info_->table_->GetTuple(r);

  if (!m.is_deleted_) {
    // insert on existing tuple, abort
    version_link->in_progress_ = false;
    txn_mgr->UpdateVersionLink(r, version_link);
    txn_->SetTainted();
    throw ExecutionException("Insert on existing tuple in InsertExecutor(InsertWithExistingIndex)");
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

void InsertExecutor::InsertWithExistingIndexUsingLock(RID r, const Tuple &t) {
  auto page_guard = table_info_->table_->AcquireTablePageWriteLock(r);  // lock the page
  auto [m, old_tuple] = table_info_->table_->GetTupleWithLockAcquired(r, page_guard.As<TablePage>());

  // check write-write conflict
  if (m.ts_ > txn_->GetReadTs() && m.ts_ != txn_->GetTransactionTempTs()) {
    txn_->SetTainted();
    throw ExecutionException("InsertExecutor(InsertWithExistingIndexUsingLock): write-write conflict");
    // just setTainted and throw exception, Abort will be called in the upper level(bustub instance)
  }

  if (!m.is_deleted_) {
    // insert on existing tuple, abort
    txn_->SetTainted();
    throw ExecutionException("Insert on existing tuple in InsertExecutor(InsertWithExistingIndexUsingLock)");
    // just setTainted and throw exception, Abort will be called in the upper level(bustub instance)
  }

  auto txn_mgr = exec_ctx_->GetTransactionManager();
  auto version_link = txn_mgr->GetVersionLink(r);
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
      UpdateTupleWithLocking(t, r, page_guard.AsMut<TablePage>());
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
      txn_mgr->UpdateVersionLink(r, VersionUndoLink::FromOptionalUndoLink(txn_->AppendUndoLog(undo_log)));
      // update the tuple on the table heap
      UpdateTupleWithLocking(t, r, page_guard.AsMut<TablePage>());
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
    txn_mgr->UpdateVersionLink(r, VersionUndoLink::FromOptionalUndoLink(txn_->AppendUndoLog(undo_log)));
    // update the tuple on the table heap
    UpdateTupleWithLocking(t, r, page_guard.AsMut<TablePage>());
  } else {
    // no version link and this tuple is created by this transaction
    UpdateTupleWithLocking(t, r, page_guard.AsMut<TablePage>());
  }

  txn_->AppendWriteSet(table_info_->oid_, r);
}

void InsertExecutor::InsertWithNewIndex(Tuple &t, RID *new_rid) {
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
      // just setTainted and throw exception, Abort will be called in the upper level(bustub instance)
    }
  }
}

}  // namespace bustub

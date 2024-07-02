//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <memory>
#include <optional>

#include "common/config.h"
#include "common/macros.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "execution/executors/delete_executor.h"
#include "type/value_factory.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  child_executor_->Init();
  auto catalog = exec_ctx_->GetCatalog();
  table_info_ = catalog->GetTable(plan_->GetTableOid());
  indexes_ = catalog->GetTableIndexes(table_info_->name_);
  txn_ = exec_ctx_->GetTransaction();
  deleted_ = false;
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (deleted_) {
    return false;
  }

  Tuple t;
  RID r;
  int count_deleted = 0;
  while (child_executor_->Next(&t, &r)) {
    // TryDelete(r);
    TryDeleteUsingLock(r);
    count_deleted++;
  }

  *tuple = Tuple{{ValueFactory::GetIntegerValue(count_deleted)}, &GetOutputSchema()};

  deleted_ = true;
  return true;
}

void DeleteExecutor::TryDelete(const RID &r) {
  auto txn_mgr = exec_ctx_->GetTransactionManager();
  auto version_link = txn_mgr->GetVersionLink(r);
  CheckAndLockVersionLink(r, txn_, txn_mgr, &version_link, table_info_);

  auto [tuple_meta, old_tuple] = table_info_->table_->GetTuple(r);
  if (tuple_meta.is_deleted_) {
    version_link->in_progress_ = false;
    txn_mgr->UpdateVersionLink(r, version_link);
    // already deleted, skip
    return;
  }

  // update undo log
  if (version_link.has_value() && version_link->prev_.IsValid()) {
    // has undo log
    if (version_link->prev_.prev_txn_ == txn_->GetTransactionId()) {
      // first undo log is created by this transaction, reuse
      auto first_undo_log = txn_mgr->GetUndoLog(version_link->prev_);
      if (!first_undo_log.is_deleted_) {
        auto new_partial_tuple = ReconstructTuple(&table_info_->schema_, old_tuple, tuple_meta, {first_undo_log});
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
      undo_log.ts_ = tuple_meta.ts_;
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
  } else if (tuple_meta.ts_ != txn_->GetTransactionTempTs()) {
    // no undo log and this tuple is not created by this transaction, create a new undo log
    UndoLog undo_log;
    undo_log.is_deleted_ = false;  // should not be deleted, or it should already be "continued" before
    undo_log.modified_fields_ = std::vector<bool>(table_info_->schema_.GetColumnCount(), true);
    undo_log.tuple_ = old_tuple;
    undo_log.ts_ = tuple_meta.ts_;

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

void DeleteExecutor::TryDeleteUsingLock(const RID &r) {
  auto page_guard = table_info_->table_->AcquireTablePageWriteLock(r);

  auto [tuple_meta, old_tuple] = table_info_->table_->GetTupleWithLockAcquired(r, page_guard.As<TablePage>());
  // check write-write conflict
  if (tuple_meta.ts_ > txn_->GetReadTs() && tuple_meta.ts_ != txn_->GetTransactionTempTs()) {
    txn_->SetTainted();
    throw ExecutionException("DeleteExecutor(TryDeleteUsingLock): write-write conflict");
    // just setTainted and throw exception, Abort will be called in the upper level(bustub instance)
  }

  if (tuple_meta.is_deleted_) {
    // already deleted, skip
    return;
  }

  auto txn_mgr = exec_ctx_->GetTransactionManager();
  auto version_link = txn_mgr->GetVersionLink(r);
  // update undo log
  if (version_link.has_value() && version_link->prev_.IsValid()) {
    // has undo log
    if (version_link->prev_.prev_txn_ == txn_->GetTransactionId()) {
      // first undo log is created by this transaction, reuse
      auto first_undo_log = txn_mgr->GetUndoLog(version_link->prev_);
      if (!first_undo_log.is_deleted_) {
        auto new_partial_tuple = ReconstructTuple(&table_info_->schema_, old_tuple, tuple_meta, {first_undo_log});
        first_undo_log.modified_fields_ = std::vector<bool>(table_info_->schema_.GetColumnCount(), true);
        BUSTUB_ASSERT(new_partial_tuple.has_value(), "ReconstructTuple should return a tuple here");
        first_undo_log.tuple_ = new_partial_tuple.value();
      }
      txn_->ModifyUndoLog(version_link->prev_.prev_log_idx_, first_undo_log);
      // delete the tuple
      DeleteTupleWithLocking(r, old_tuple, page_guard.AsMut<TablePage>());
    } else {
      // first undo log is not created by this transaction, create a new undo logDD
      UndoLog undo_log;
      undo_log.is_deleted_ = false;  // should not be deleted, or it should already be "continued" before
      undo_log.modified_fields_ = std::vector<bool>(table_info_->schema_.GetColumnCount(), true);
      undo_log.tuple_ = old_tuple;
      undo_log.ts_ = tuple_meta.ts_;
      undo_log.prev_version_ = version_link->prev_;  // link to the previous version

      // need to update the version link before mark the tuple as deleted on the table heap
      txn_mgr->UpdateVersionLink(r, VersionUndoLink::FromOptionalUndoLink(txn_->AppendUndoLog(undo_log)));
      // delete the tuple on the table heap
      DeleteTupleWithLocking(r, old_tuple, page_guard.AsMut<TablePage>());
    }
  } else if (tuple_meta.ts_ != txn_->GetTransactionTempTs()) {
    // no undo log and this tuple is not created by this transaction, create a new undo log
    UndoLog undo_log;
    undo_log.is_deleted_ = false;  // should not be deleted, or it should already be "continued" before
    undo_log.modified_fields_ = std::vector<bool>(table_info_->schema_.GetColumnCount(), true);
    undo_log.tuple_ = old_tuple;
    undo_log.ts_ = tuple_meta.ts_;

    // need to update the version link before mark the tuple as deleted on the table heap
    txn_mgr->UpdateVersionLink(r, VersionUndoLink::FromOptionalUndoLink(txn_->AppendUndoLog(undo_log)));
    // delete the tuple on the table heap
    DeleteTupleWithLocking(r, old_tuple, page_guard.AsMut<TablePage>());
  } else {
    // no version link and this tuple is created by this transaction
    DeleteTupleWithLocking(r, old_tuple, page_guard.AsMut<TablePage>());
  }

  txn_->AppendWriteSet(plan_->GetTableOid(), r);  // append to write set
}

}  // namespace bustub

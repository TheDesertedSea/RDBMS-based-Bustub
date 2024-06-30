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
        InsertWithExistingIndex(result[0], t);
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
    throw ExecutionException("Write-write conflict detected in InsertExecutor(InsertWithExistingIndex)");
  }

  if (!m.is_deleted_) {
    // insert on existing tuple
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

void InsertExecutor::InsertWithNewIndex(Tuple &t, RID *new_rid) {
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
      throw ExecutionException("InsertWithNewIndex: Another transaction has inserted the same key, aborting");
    }
  }
}

}  // namespace bustub

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
  deleted_ = false;
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (deleted_) {
    return false;
  }

  auto catalog = exec_ctx_->GetCatalog();
  auto table_info = catalog->GetTable(plan_->GetTableOid());
  auto indexes = catalog->GetTableIndexes(table_info->name_);
  auto txn = exec_ctx_->GetTransaction();
  auto txn_mgr = exec_ctx_->GetTransactionManager();

  Tuple t;
  RID r;
  int count_deleted = 0;
  while (child_executor_->Next(&t, &r)) {
    // check write-write conflict
    TupleMeta tuple_meta = table_info->table_->GetTupleMeta(r);
    if (tuple_meta.ts_ > txn->GetReadTs() && tuple_meta.ts_ != txn->GetTransactionTempTs()) {
      txn->SetTainted();
      throw ExecutionException("Write-write conflict detected in DeleteExecutor");
    }

    if (tuple_meta.is_deleted_) {
      // already deleted, skip
      continue;
    }

    // update undo log
    auto first_undo_link = txn_mgr->GetUndoLink(r);
    if (first_undo_link.has_value() && first_undo_link->IsValid()) {
      // has undo log
      if (first_undo_link->prev_txn_ == txn->GetTransactionId()) {
        // first undo log is created by this transaction, reuse
        auto first_undo_log = txn_mgr->GetUndoLog(first_undo_link.value());
        if (!first_undo_log.is_deleted_) {
          first_undo_log.modified_fields_ = std::vector<bool>(table_info->schema_.GetColumnCount(), true);
          auto new_partial_tuple = ReconstructTuple(&table_info->schema_, t, tuple_meta, {first_undo_log});
          BUSTUB_ASSERT(new_partial_tuple.has_value(), "ReconstructTuple should return a tuple here");
          first_undo_log.tuple_ = new_partial_tuple.value();
        }
        txn->ModifyUndoLog(first_undo_link->prev_log_idx_, first_undo_log);
      } else {
        // first undo log is not created by this transaction, create a new undo log
        UndoLog undo_log;
        undo_log.is_deleted_ = false;  // should not be deleted, or it should already be "continued" before
        undo_log.modified_fields_ = std::vector<bool>(table_info->schema_.GetColumnCount(), true);
        undo_log.tuple_ = t;
        undo_log.ts_ = tuple_meta.ts_;
        undo_log.prev_version_ = first_undo_link.value();  // link to the previous version
        txn_mgr->UpdateUndoLink(r, txn->AppendUndoLog(undo_log));
      }
    } else if (tuple_meta.ts_ != txn->GetTransactionTempTs()) {
      // no undo log and this tuple is not created by this transaction, create a new undo log
      UndoLog undo_log;
      undo_log.is_deleted_ = false;  // should not be deleted, or it should already be "continued" before
      undo_log.modified_fields_ = std::vector<bool>(table_info->schema_.GetColumnCount(), true);
      undo_log.tuple_ = t;
      undo_log.ts_ = tuple_meta.ts_;
      txn_mgr->UpdateUndoLink(r, txn->AppendUndoLog(undo_log));
    }

    // delete the tuple
    tuple_meta.is_deleted_ = true;
    tuple_meta.ts_ = txn->GetTransactionTempTs();
    table_info->table_->UpdateTupleMeta(tuple_meta, r);

    for (auto &index_info : indexes) {
      // Delete the old index entry then insert the new index entry
      index_info->index_->DeleteEntry(
          t.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()), r,
          exec_ctx_->GetTransaction());
    }

    txn->AppendWriteSet(plan_->GetTableOid(), r);  // append to write set
    count_deleted++;
  }

  *tuple = Tuple{{ValueFactory::GetIntegerValue(count_deleted)}, &GetOutputSchema()};

  deleted_ = true;
  return true;
}

}  // namespace bustub

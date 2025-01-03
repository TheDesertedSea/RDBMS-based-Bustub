//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "storage/table/tuple.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), table_iter_ptr_(nullptr) {}

void SeqScanExecutor::Init() {
  table_iter_ptr_ = std::make_unique<TableIterator>(
      exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_->MakeEagerIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto predicate = plan_->filter_predicate_;
  auto txn = exec_ctx_->GetTransaction();
  auto txn_manager = exec_ctx_->GetTransactionManager();
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  auto read_ts = txn->GetReadTs();

  while (!table_iter_ptr_->IsEnd()) {
    auto next_rid = table_iter_ptr_->GetRID();
    auto page_guard = table_info->table_->AcquireTablePageReadLock(next_rid);
    auto [next_meta, next_tuple] = table_info->table_->GetTupleWithLockAcquired(next_rid, page_guard.As<TablePage>());

    ++(*table_iter_ptr_);

    if ((next_meta.ts_ <= read_ts) || (next_meta.ts_ == txn->GetTransactionTempTs())) {
      // can be seen by current transaction
      if (!next_meta.is_deleted_) {
        *tuple = next_tuple;
        *rid = next_rid;
        if (predicate == nullptr || predicate->Evaluate(tuple, GetOutputSchema()).GetAs<bool>()) {
          return true;
        }
      }
    } else {
      // need to check undo logs
      auto heap_ts = next_meta.ts_;                               // the timestamp on table heap
      auto version_link = txn_manager->GetVersionLink(next_rid);  // get the first undo link
      auto undo_link = version_link.has_value() ? std::optional<UndoLink>(version_link->prev_) : std::nullopt;
      std::vector<UndoLog> undo_logs;
      bool has_visible_version = false;  // whether there's a visible version
      while (undo_link.has_value() && undo_link->IsValid()) {
        // only continue if the undo link is valid, otherwise there's no previous version
        auto undo_log = txn_manager->GetUndoLog(undo_link.value());
        undo_link = undo_log.prev_version_;
        if (undo_log.ts_ <= read_ts) {
          // this version can be seen by current transaction
          has_visible_version = !undo_log.is_deleted_;
          undo_logs.push_back(std::move(undo_log));
          break;
        }
        if (undo_log.ts_ != heap_ts) {
          // there may be a small amount of time when the table heap contains a tuple with the same timestamp as the
          // first undo log, they are duplicates. Only add to undo_logs if the timestamps are different.
          undo_logs.push_back(std::move(undo_log));
        }
      }
      if (has_visible_version) {
        // found a visible version, reconstruct the tuple
        auto reconstruct_result = ReconstructTuple(&plan_->OutputSchema(), next_tuple, next_meta, undo_logs);
        if (reconstruct_result.has_value()) {
          auto reconstructed_tuple = reconstruct_result.value();
          if (predicate == nullptr || predicate->Evaluate(&reconstructed_tuple, GetOutputSchema()).GetAs<bool>()) {
            *tuple = reconstructed_tuple;
            *rid = next_rid;
            return true;
          }
        }
      }
    }
  }

  return false;
}

}  // namespace bustub

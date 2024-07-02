//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "storage/table/tuple.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() { scan_finished_ = false; }

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (scan_finished_) {
    return false;
  }

  auto index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  auto index = index_info->index_.get();
  auto txn = exec_ctx_->GetTransaction();
  auto txn_manager = exec_ctx_->GetTransactionManager();
  auto read_ts = txn->GetReadTs();

  std::vector<RID> rids;
  Tuple dummy_tuple;
  Value key_value = plan_->pred_key_->Evaluate(&dummy_tuple, GetOutputSchema());
  index->ScanKey(Tuple({key_value}, index->GetKeySchema()), &rids, exec_ctx_->GetTransaction());

  if (rids.empty()) {
    scan_finished_ = true;
    return false;
  }

  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  auto page_guard = table_info->table_->AcquireTablePageReadLock(rids[0]);
  auto [m, t] = table_info->table_->GetTupleWithLockAcquired(rids[0], page_guard.As<TablePage>());
  if ((m.ts_ <= read_ts) || (m.ts_ == txn->GetTransactionTempTs())) {
    // can be seen by current transaction
    if (!m.is_deleted_) {
      *tuple = t;
      *rid = rids[0];
      scan_finished_ = true;
      return true;
    }
  } else {
    // need to check undo logs
    auto heap_ts = m.ts_;                                      // the timestamp on table heap
    auto version_link = txn_manager->GetVersionLink(rids[0]);  // get the first undo link
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
      auto reconstruct_result = ReconstructTuple(&plan_->OutputSchema(), t, m, undo_logs);
      if (reconstruct_result.has_value()) {
        *tuple = reconstruct_result.value();
        *rid = rids[0];
        scan_finished_ = true;
        return true;
      }
    }
  }

  scan_finished_ = true;
  return false;
}
}  // namespace bustub

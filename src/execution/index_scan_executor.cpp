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
#include <iostream>
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
  std::vector<RID> rids;
  Tuple dummy_tuple;
  Value key_value = plan_->pred_key_->Evaluate(&dummy_tuple, GetOutputSchema());
  index->ScanKey(Tuple({key_value}, index->GetKeySchema()), &rids, exec_ctx_->GetTransaction());

  if (rids.empty()) {
    scan_finished_ = true;
    return false;
  }

  *rid = rids[0];
  auto result_pair = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_)->table_->GetTuple(*rid);
  if (result_pair.first.is_deleted_) {
    scan_finished_ = true;
    return false;
  }

  *tuple = result_pair.second;
  scan_finished_ = true;
  return true;
}
}  // namespace bustub

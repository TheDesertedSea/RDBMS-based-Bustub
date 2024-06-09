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
#include "storage/table/tuple.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), table_iter_ptr_(nullptr) {}

void SeqScanExecutor::Init() {
  table_iter_ptr_ =
      std::make_unique<TableIterator>(exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto predicate = plan_->filter_predicate_;

  while (!table_iter_ptr_->IsEnd()) {
    auto next_tuple = table_iter_ptr_->GetTuple();
    ++(*table_iter_ptr_);

    if (!next_tuple.first.is_deleted_) {
      *tuple = next_tuple.second;
      *rid = table_iter_ptr_->GetRID();
      if (predicate == nullptr || predicate->Evaluate(tuple, GetOutputSchema()).GetAs<bool>()) {
        return true;
      }
    }
  }

  return false;
}

}  // namespace bustub

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
#include "common/exception.h"
#include "common/macros.h"
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
  inserted_ = false;
  child_executor_->Init();
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (inserted_) {
    return false;
  }

  auto catalog = exec_ctx_->GetCatalog();
  auto table_info = catalog->GetTable(plan_->GetTableOid());
  auto indexes = catalog->GetTableIndexes(table_info->name_);
  auto txn = exec_ctx_->GetTransaction();

  Tuple t;
  RID r;
  int count_inserted = 0;
  while (child_executor_->Next(&t, &r)) {
    // step 1 check if the tuple already exists in the index
    for (auto &index_info : indexes) {
      std::vector<RID> result;
      index_info->index_->ScanKey(
          t.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()), &result,
          txn);
      if (!result.empty()) {
        txn->SetTainted();
        throw ExecutionException("InsertExecutor::Next: Duplicate key found in index");
      }
    }

    // step 2 create a tuple on the table heap with a transaction temporary timestamp
    TupleMeta meta;
    meta.is_deleted_ = false;
    meta.ts_ = txn->GetTransactionTempTs();
    auto new_rid = table_info->table_->InsertTuple(meta, t, exec_ctx_->GetLockManager(), txn, plan_->GetTableOid());
    BUSTUB_ASSERT(new_rid.has_value(), "InsertTuple should return a RID on success");

    // step 3 insert the tuple into the index
    for (auto &index_info : indexes) {
      auto result = index_info->index_->InsertEntry(
          t.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()),
          new_rid.value(), exec_ctx_->GetTransaction());
      if (!result) {
        txn->SetTainted();
        throw ExecutionException("InsertExecutor::Next: InsertEntry failed");
      }
    }

    txn->AppendWriteSet(plan_->GetTableOid(), new_rid.value());  // add to write set
    count_inserted++;
  }
  *tuple = Tuple{{ValueFactory::GetIntegerValue(count_inserted)}, &GetOutputSchema()};

  inserted_ = true;
  return true;
}

}  // namespace bustub

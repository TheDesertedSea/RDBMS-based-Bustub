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
#include "storage/table/tuple.h"

#include "execution/executors/update_executor.h"
#include "type/value_factory.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() {
  child_executor_->Init();
  updated_ = false;
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (updated_) {
    return false;
  }

  auto catalog = exec_ctx_->GetCatalog();
  auto table_info = catalog->GetTable(plan_->GetTableOid());
  auto indexes = catalog->GetTableIndexes(table_info->name_);

  Tuple t;
  RID r;
  int count_updated = 0;
  while (child_executor_->Next(&t, &r)) {
    // Delete the old tuple then insert the new tuple
    TupleMeta meta_deleted;
    meta_deleted.is_deleted_ = true;
    table_info->table_->UpdateTupleMeta(meta_deleted, r);

    // generate the new tuple
    std::vector<Value> values;
    for (auto target_expr : plan_->target_expressions_) {
      values.push_back(target_expr->Evaluate(&t, child_executor_->GetOutputSchema()));
    }
    Tuple new_tuple(values, &child_executor_->GetOutputSchema());

    TupleMeta meta_inserted;
    meta_inserted.is_deleted_ = false;
    table_info->table_->InsertTuple(meta_inserted, new_tuple, exec_ctx_->GetLockManager(), exec_ctx_->GetTransaction(),
                                    plan_->GetTableOid());

    for (auto &index_info : indexes) {
      // Delete the old index entry then insert the new index entry
      index_info->index_->DeleteEntry(
          t.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()), r,
          exec_ctx_->GetTransaction());

      auto result = index_info->index_->InsertEntry(
          new_tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()), r,
          exec_ctx_->GetTransaction());
      if (!result) {
        return false;
      }
    }
    count_updated++;
  }
  *tuple = Tuple{{ValueFactory::GetIntegerValue(count_updated)}, &GetOutputSchema()};

  updated_ = true;
  return true;
}

}  // namespace bustub

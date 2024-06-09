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

  Tuple t;
  RID r;
  int count_deleted = 0;
  while (child_executor_->Next(&t, &r)) {
    TupleMeta meta_deleted;
    meta_deleted.is_deleted_ = true;
    table_info->table_->UpdateTupleMeta(meta_deleted, r);

    for (auto &index_info : indexes) {
      // Delete the old index entry then insert the new index entry
      index_info->index_->DeleteEntry(
          t.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()), r,
          exec_ctx_->GetTransaction());
    }
    count_deleted++;
  }

  *tuple = Tuple{{ValueFactory::GetIntegerValue(count_deleted)}, &GetOutputSchema()};

  deleted_ = true;
  return true;
}

}  // namespace bustub

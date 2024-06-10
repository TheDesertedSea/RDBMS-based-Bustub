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

  Tuple t;
  RID r;
  int count_inserted = 0;
  while (child_executor_->Next(&t, &r)) {
    TupleMeta meta;
    meta.is_deleted_ = false;
    auto new_rid = table_info->table_->InsertTuple(meta, t, exec_ctx_->GetLockManager(), exec_ctx_->GetTransaction(),
                                                   plan_->GetTableOid());

    for (auto &index_info : indexes) {
      auto result = index_info->index_->InsertEntry(
          t.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()),
          new_rid.value(), exec_ctx_->GetTransaction());
      if (!result) {
        return false;
      }
    }
    count_inserted++;
  }
  *tuple = Tuple{{ValueFactory::GetIntegerValue(count_inserted)}, &GetOutputSchema()};

  inserted_ = true;
  return true;
}

}  // namespace bustub

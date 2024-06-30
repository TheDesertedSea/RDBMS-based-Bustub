#include "execution/execution_common.h"
#include <sys/types.h>
#include <cstdint>
#include <optional>
#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/macros.h"
#include "concurrency/transaction_manager.h"
#include "fmt/core.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple> {
  /** Initial data */
  std::vector<Value> values;
  auto is_deleted = base_meta.is_deleted_;
  for (uint32_t idx = 0; idx < schema->GetColumnCount(); idx++) {
    values.push_back(base_tuple.GetValue(schema, idx));
  }

  for (auto &undo_log : undo_logs) {
    /** Generate partial schema */
    std::vector<Column> modified_columns;
    std::vector<uint32_t> modified_idxs;
    for (uint32_t idx = 0; idx < schema->GetColumnCount(); idx++) {
      if (undo_log.modified_fields_[idx]) {
        modified_columns.push_back(schema->GetColumn(idx));
        modified_idxs.push_back(idx);
      }
    }
    auto partial_schema = Schema(modified_columns);

    /** Apply undo */
    for (uint32_t i = 0; i < modified_idxs.size(); i++) {
      values[modified_idxs[i]] = undo_log.tuple_.GetValue(&partial_schema, i);
    }
    is_deleted = undo_log.is_deleted_;
  }

  if (is_deleted) {
    return std::nullopt;
  }

  return Tuple(values, schema);
}

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap) {
  // always use stderr for printing logs...
  fmt::println(stderr, "debug_hook: {}", info);
  auto watermark = txn_mgr->GetWatermark();

  auto itr = table_heap->MakeIterator();
  while (!itr.IsEnd()) {
    auto [tuple_meta, tuple] = itr.GetTuple();
    auto rid = itr.GetRID();
    auto ts = (tuple_meta.ts_ & TXN_START_ID) != 0 ? "txn" + std::to_string(tuple_meta.ts_ ^ TXN_START_ID)
                                                   : std::to_string(tuple_meta.ts_);
    fmt::print(stderr, "RID={}/{}, ts={} {} tuple={}\n", rid.GetPageId(), rid.GetSlotNum(), ts,
               tuple_meta.is_deleted_ ? "<del>" : "", tuple.ToString(&table_info->schema_));

    if (tuple_meta.ts_ > watermark) {
      // table heap tuple is not visible to all transactions, we need to check the undo log
      /** Initial Values */
      std::vector<Value> values;
      for (uint32_t idx = 0; idx < table_info->schema_.GetColumnCount(); idx++) {
        values.push_back(tuple.GetValue(&table_info->schema_, idx));
      }
      auto undo_link = txn_mgr->GetUndoLink(rid);
      while (undo_link.has_value() && undo_link->IsValid()) {
        auto undo_log = txn_mgr->GetUndoLog(undo_link.value());
        auto txn_id = undo_link->prev_txn_ ^ TXN_START_ID;

        /** Generate partial schema */
        std::vector<Column> modified_columns;
        std::vector<uint32_t> modified_idxs;
        for (uint32_t idx = 0; idx < table_info->schema_.GetColumnCount(); idx++) {
          if (undo_log.modified_fields_[idx]) {
            modified_columns.push_back(table_info->schema_.GetColumn(idx));
            modified_idxs.push_back(idx);
          }
        }
        auto partial_schema = Schema(modified_columns);

        /** Apply undo */
        for (uint32_t i = 0; i < modified_idxs.size(); i++) {
          values[modified_idxs[i]] = undo_log.tuple_.GetValue(&partial_schema, i);
        }

        Tuple temp_tuple(values, &table_info->schema_);
        fmt::print(stderr, "  txn{}@{} {} ts={}\n", txn_id, undo_link->prev_log_idx_,
                   undo_log.is_deleted_ ? "<del>" : temp_tuple.ToString(&table_info->schema_), undo_log.ts_);

        if (undo_log.ts_ <= watermark) {
          // this is the deepest version that is visible to all transactions
          break;
        }
        undo_link = undo_log.prev_version_;
      }
    }  // meta.ts_ <= water_mark, table heap tuple is already visible to all transactions, undo log is not needed

    ++itr;
  }

  // We recommend implementing this function as traversing the table heap and print the version chain. An example output
  // of our reference solution:
  //
  // debug_hook: before verify scan
  // RID=0/0 ts=txn8 tuple=(1, <NULL>, <NULL>)
  //   txn8@0 (2, _, _) ts=1
  // RID=0/1 ts=3 tuple=(3, <NULL>, <NULL>)
  //   txn5@0 <del> ts=2
  //   txn3@0 (4, <NULL>, <NULL>) ts=1
  // RID=0/2 ts=4 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn7@0 (5, <NULL>, <NULL>) ts=3
  // RID=0/3 ts=txn6 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn6@0 (6, <NULL>, <NULL>) ts=2
  //   txn3@1 (7, _, _) ts=1
}

auto GeneratePartialTuple(const Schema &schema, const Tuple &old_tuple, const std::vector<Value> &new_values,
                          std::vector<bool> &modified_fields) -> Tuple {
  auto column_count = schema.GetColumnCount();
  std::vector<Column> modified_columns;
  std::vector<Value> modified;
  for (uint32_t i = 0; i != column_count; i++) {
    auto old_value = old_tuple.GetValue(&schema, i);
    if (!old_value.CompareExactlyEquals(new_values[i])) {
      modified_fields[i] = true;
      modified_columns.push_back(schema.GetColumn(i));
      modified.emplace_back(old_value);
    }
  }

  Schema partial_schema = Schema(modified_columns);
  return {modified, &partial_schema};
}

auto GeneratePartialTuple(const Schema &schema, const Tuple &old_tuple, const Tuple &new_tuple,
                          std::vector<bool> &modified_fields) -> Tuple {
  auto column_count = schema.GetColumnCount();
  std::vector<Column> modified_columns;
  std::vector<Value> modified;
  for (uint32_t i = 0; i != column_count; i++) {
    auto old_value = old_tuple.GetValue(&schema, i);
    if (!old_value.CompareExactlyEquals(new_tuple.GetValue(&schema, i))) {
      modified_fields[i] = true;
      modified_columns.push_back(schema.GetColumn(i));
      modified.emplace_back(old_value);
    }
  }

  Schema partial_schema = Schema(modified_columns);
  return {modified, &partial_schema};
}

auto MergeParitalTuple(const Schema &schema, const Tuple &orig_tuple, const std::vector<Value> &new_values,
                       const Tuple &partial_tuple_old, const std::vector<bool> &modified_fields_old,
                       std::vector<bool> &merged_modified_fields) -> Tuple {
  // Generate the old partial schema
  std::vector<Column> partial_columns_old;
  for (uint32_t i = 0; i != schema.GetColumnCount(); i++) {
    if (modified_fields_old[i]) {
      partial_columns_old.push_back(schema.GetColumn(i));
    }
  }
  Schema partial_schema_old = Schema(partial_columns_old);

  // traverse all columns to find out merged columns and values
  auto column_count = schema.GetColumnCount();
  std::vector<Column> merged_columns;
  std::vector<Value> merged_values;
  uint32_t partial_idx_old = 0;
  for (uint32_t i = 0; i != column_count; i++) {
    if (modified_fields_old[i]) {
      // already in the old partial tuple
      merged_modified_fields[i] = true;
      merged_columns.push_back(schema.GetColumn(i));
      merged_values.push_back(partial_tuple_old.GetValue(&partial_schema_old, partial_idx_old));
      partial_idx_old++;
    } else {
      // not in the old partial tuple, check if it is modified
      auto orig_value = orig_tuple.GetValue(&schema, i);
      if (!orig_value.CompareExactlyEquals(new_values[i])) {
        merged_modified_fields[i] = true;
        merged_columns.push_back(schema.GetColumn(i));
        merged_values.push_back(orig_value);
      }
    }
  }

  Schema merged_schema = Schema(merged_columns);
  return {merged_values, &merged_schema};
}

auto MergeParitalTuple(const Schema &schema, const Tuple &orig_tuple, const Tuple &new_tuple,
                       const Tuple &partial_tuple_old, const std::vector<bool> &modified_fields_old,
                       std::vector<bool> &merged_modified_fields) -> Tuple {
  // Generate the old partial schema
  std::vector<Column> partial_columns_old;
  for (uint32_t i = 0; i != schema.GetColumnCount(); i++) {
    if (modified_fields_old[i]) {
      partial_columns_old.push_back(schema.GetColumn(i));
    }
  }
  Schema partial_schema_old = Schema(partial_columns_old);

  // traverse all columns to find out merged columns and values
  auto column_count = schema.GetColumnCount();
  std::vector<Column> merged_columns;
  std::vector<Value> merged_values;
  uint32_t partial_idx_old = 0;
  for (uint32_t i = 0; i != column_count; i++) {
    if (modified_fields_old[i]) {
      // already in the old partial tuple
      merged_modified_fields[i] = true;
      merged_columns.push_back(schema.GetColumn(i));
      merged_values.push_back(partial_tuple_old.GetValue(&partial_schema_old, partial_idx_old));
      partial_idx_old++;
    } else {
      // not in the old partial tuple, check if it is modified
      auto orig_value = orig_tuple.GetValue(&schema, i);
      if (!orig_value.CompareExactlyEquals(new_tuple.GetValue(&schema, i))) {
        merged_modified_fields[i] = true;
        merged_columns.push_back(schema.GetColumn(i));
        merged_values.push_back(orig_value);
      }
    }
  }

  Schema merged_schema = Schema(merged_columns);
  return {merged_values, &merged_schema};
}

void LockVersionLink(const RID &rid, Transaction *txn, TransactionManager *txn_mgr,
                     std::optional<VersionUndoLink> *version_link) {
  if (version_link->has_value()) {
    if ((*version_link)->in_progress_) {
      txn->SetTainted();
      throw ExecutionException("InsertWithExistingIndex: version link is being updated, aborting transaction");
    }
    (*version_link)->in_progress_ = true;
    if ((*version_link)->prev_.IsValid()) {
      if (!txn_mgr->UpdateVersionLink(
              rid, (*version_link),
              [old_log_head_txn_id = (*version_link)->prev_.prev_txn_](std::optional<VersionUndoLink> v) {
                return v.has_value() && v->prev_.prev_txn_ == old_log_head_txn_id;
              })) {
        txn->SetTainted();
        throw ExecutionException("InsertWithExistingIndex: Write-write conflict detected(InsertWithExistingIndex 1)");
      }
    } else if (!txn_mgr->UpdateVersionLink(rid, (*version_link), [](std::optional<VersionUndoLink> v) {
                 return v.has_value() && !v->prev_.IsValid();
               })) {
      txn->SetTainted();
      throw ExecutionException("InsertExecutor::Next: Write-write conflict detected(InsertWithExistingIndex 2)");
    }
  } else {
    *version_link = VersionUndoLink{UndoLink{INVALID_TXN_ID, 0}, true};
    if (!txn_mgr->UpdateVersionLink(rid, *version_link,
                                    [](std::optional<VersionUndoLink> v) { return !v.has_value(); })) {
      txn->SetTainted();
      throw ExecutionException("InsertExecutor::Next: Write-write conflict detected(InsertWithExistingIndex 3)");
    }
  }
}

void UpdateTupleAndVersionLink(Transaction *txn, TransactionManager *txn_mgr, const Tuple &old_tuple, TupleMeta meta,
                               const RID &rid, const Tuple &tuple, bool is_delete,
                               std::optional<VersionUndoLink> &version_link, TableInfo *table_info) {
  if (version_link.has_value() && version_link->prev_.IsValid()) {
    // has undo log
    if (version_link->prev_.prev_txn_ == txn->GetTransactionId()) {
      // first undo log is created by this transaction, reuse
      auto first_undo_log = txn_mgr->GetUndoLog(version_link->prev_);
      if (!first_undo_log.is_deleted_) {
        if (is_delete) {
          auto new_partial_tuple = ReconstructTuple(&table_info->schema_, old_tuple, meta, {first_undo_log});
          first_undo_log.modified_fields_ = std::vector<bool>(table_info->schema_.GetColumnCount(), true);
          BUSTUB_ASSERT(new_partial_tuple.has_value(), "ReconstructTuple should return a tuple here");
          first_undo_log.tuple_ = new_partial_tuple.value();
        } else {
          std::vector<bool> merged_modified_fields(table_info->schema_.GetColumnCount(), false);
          first_undo_log.tuple_ = MergeParitalTuple(table_info->schema_, old_tuple, tuple, first_undo_log.tuple_,
                                                    first_undo_log.modified_fields_, merged_modified_fields);
          first_undo_log.modified_fields_ = merged_modified_fields;
        }
      }
      txn->ModifyUndoLog(version_link->prev_.prev_log_idx_, first_undo_log);

      // step 2 create a tuple on the table heap with a transaction temporary timestamp
      meta.is_deleted_ = is_delete;
      meta.ts_ = txn->GetTransactionTempTs();
      if (is_delete) {
        table_info->table_->UpdateTupleMeta(meta, rid);
      } else {
        table_info->table_->UpdateTupleInPlace(meta, tuple, rid);
      }

      version_link->in_progress_ = false;
      txn_mgr->UpdateVersionLink(rid, version_link);
    } else {
      // first undo log is not created by this transaction, create a new undo log
      auto undo_log = UndoLog();
      undo_log.is_deleted_ = meta.is_deleted_;  // original tuple may be deleted, some transaction may delete it and
                                                // commit between the child executor and this executor
      undo_log.modified_fields_ = std::vector<bool>(table_info->schema_.GetColumnCount(), is_delete);
      if (is_delete) {
        undo_log.tuple_ = old_tuple;
      } else {
        undo_log.tuple_ = meta.is_deleted_
                              ? Tuple()
                              : GeneratePartialTuple(table_info->schema_, old_tuple, tuple, undo_log.modified_fields_);
      }
      undo_log.prev_version_ = version_link->prev_;
      undo_log.ts_ = meta.ts_;

      // step 2 create a tuple on the table heap with a transaction temporary timestamp
      meta.is_deleted_ = is_delete;
      meta.ts_ = txn->GetTransactionTempTs();
      if (is_delete) {
        table_info->table_->UpdateTupleMeta(meta, rid);
      } else {
        table_info->table_->UpdateTupleInPlace(meta, tuple, rid);
      }

      txn_mgr->UpdateVersionLink(rid, VersionUndoLink::FromOptionalUndoLink(txn->AppendUndoLog(undo_log)));
    }
  } else if (meta.ts_ != txn->GetTransactionTempTs()) {
    // no undo log and this tuple is not created by this transaction, create a new undo log
    auto undo_log = UndoLog();
    undo_log.is_deleted_ = meta.is_deleted_;  // original tuple may be deleted
    undo_log.modified_fields_ = std::vector<bool>(table_info->schema_.GetColumnCount(), is_delete);
    if (is_delete) {
      undo_log.tuple_ = old_tuple;
    } else {
      undo_log.tuple_ = meta.is_deleted_
                            ? Tuple()
                            : GeneratePartialTuple(table_info->schema_, old_tuple, tuple, undo_log.modified_fields_);
    }
    undo_log.ts_ = meta.ts_;

    // step 2 create a tuple on the table heap with a transaction temporary timestamp
    meta.is_deleted_ = is_delete;
    meta.ts_ = txn->GetTransactionTempTs();
    if (is_delete) {
      table_info->table_->UpdateTupleMeta(meta, rid);
    } else {
      table_info->table_->UpdateTupleInPlace(meta, tuple, rid);
    }

    txn_mgr->UpdateVersionLink(rid, VersionUndoLink::FromOptionalUndoLink(txn->AppendUndoLog(undo_log)));
  }
}
}  // namespace bustub

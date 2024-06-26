#pragma once

#include <string>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "concurrency/transaction.h"
#include "storage/table/tuple.h"

namespace bustub {

auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple>;

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap);

/**
 * Generate a partial tuple from the old tuple and new values.
 *
 * @param schema the schema of the tuple
 * @param old_tuple the old tuple
 * @param new_values the new values
 * @param modified_fields [out] the modified fields
 * @return the partial tuple
 */
auto GeneratePartialTuple(const Schema &schema, const Tuple &old_tuple, const std::vector<Value> &new_values,
                          std::vector<bool> &modified_fields) -> Tuple;

/**
 * Merge the partial tuple with the old partial tuple.
 *
 * @param schema the schema of the original tuple
 * @param orig_tuple the original tuple
 * @param new_values the new values
 * @param partial_tuple_old the old partial tuple
 * @param modified_fields_old the modified fields of the old partial tuple
 * @param merged_modified_fields [out] the merged modified fields
 * @return the merged partial tuple
 */
auto MergeParitalTuple(const Schema &schema, const Tuple &orig_tuple, const std::vector<Value> &new_values,
                       const Tuple &partial_tuple_old, const std::vector<bool> &modified_fields_old,
                       std::vector<bool> &merged_modified_fields) -> Tuple;

// Add new functions as needed... You are likely need to define some more functions.
//
// To give you a sense of what can be shared across executors / transaction manager, here are the
// list of helper function names that we defined in the reference solution. You should come up with
// your own when you go through the process.
// * CollectUndoLogs
// * WalkUndoLogs
// * Modify
// * IsWriteWriteConflict
// * GenerateDiffLog
// * GenerateNullTupleForSchema
// * GetUndoLogSchema
//
// We do not provide the signatures for these functions because it depends on the your implementation
// of other parts of the system. You do not need to define the same set of helper functions in
// your implementation. Please add your own ones as necessary so that you do not need to write
// the same code everywhere.

}  // namespace bustub

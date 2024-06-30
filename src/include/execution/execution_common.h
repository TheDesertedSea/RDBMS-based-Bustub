#pragma once

#include <string>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
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
 * Generate a partial tuple from the old tuple and new values.
 *
 * @param schema the schema of the tuple
 * @param old_tuple the old tuple
 * @param new_tuple the new tuple
 * @param modified_fields [out] the modified fields
 * @return the partial tuple
 */
auto GeneratePartialTuple(const Schema &schema, const Tuple &old_tuple, const Tuple &new_tuple,
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

/**
 * Merge the partial tuple with the old partial tuple.
 *
 * @param schema the schema of the original tuple
 * @param orig_tuple the original tuple
 * @param new_tuple the new tuple
 * @param partial_tuple_old the old partial tuple
 * @param modified_fields_old the modified fields of the old partial tuple
 * @param merged_modified_fields [out] the merged modified fields
 * @return the merged partial tuple
 */
auto MergeParitalTuple(const Schema &schema, const Tuple &orig_tuple, const Tuple &new_tuple,
                       const Tuple &partial_tuple_old, const std::vector<bool> &modified_fields_old,
                       std::vector<bool> &merged_modified_fields) -> Tuple;

/**
 * Try to lock the version link's in_progress_ flag.
 * If failed, set txn taited and throw an exception.
 *
 * @param rid the record id
 * @param txn the transaction
 * @param txn_mgr the transaction manager
 * @param version_link [in/out] the version link
 * @param has_version_link [out] whether a valid version link exists
 */
void LockVersionLink(const RID &rid, Transaction *txn, TransactionManager *txn_mgr,
                     std::optional<VersionUndoLink> *version_link);

/**
 * Update the tuple and version link.
 * After this, version link's in_progress_ flag should be set to false.
 *
 * @param txn the transaction
 * @param txn_mgr the transaction manager
 * @param old_tuple the old tuple
 * @param meta the old tuple meta
 * @param rid the record id
 * @param tuple the new tuple
 * @param is_delete whether this is a delete operation
 * @param version_link the version link
 * @param table_info the table info
 */
void UpdateTupleAndVersionLink(Transaction *txn, TransactionManager *txn_mgr, const Tuple &old_tuple, TupleMeta meta,
                               const RID &rid, const Tuple &tuple, bool is_delete,
                               std::optional<VersionUndoLink> &version_link, TableInfo *table_info);

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

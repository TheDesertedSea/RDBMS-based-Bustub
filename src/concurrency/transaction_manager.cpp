//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "execution/execution_common.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto TransactionManager::Begin(IsolationLevel isolation_level) -> Transaction * {
  std::unique_lock<std::shared_mutex> l(txn_map_mutex_);
  auto txn_id = next_txn_id_++;
  auto txn = std::make_unique<Transaction>(txn_id, isolation_level);
  auto *txn_ref = txn.get();
  txn_map_.insert(std::make_pair(txn_id, std::move(txn)));

  // TODO(fall2023): set the timestamps here. Watermark updated below.
  txn_ref->read_ts_.store(last_commit_ts_);

  running_txns_.AddTxn(txn_ref->read_ts_);
  return txn_ref;
}

auto TransactionManager::VerifyTxn(Transaction *txn) -> bool { return true; }

auto TransactionManager::Commit(Transaction *txn) -> bool {
  std::unique_lock<std::mutex> commit_lck(commit_mutex_);

  // TODO(fall2023): acquire commit ts!
  auto commit_ts = last_commit_ts_ + 1;  // do not incre last_commit_ts now

  if (txn->state_ != TransactionState::RUNNING) {
    throw Exception("txn not in running state");
  }

  if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    if (!VerifyTxn(txn)) {
      commit_lck.unlock();
      Abort(txn);
      return false;
    }
  }

  // TODO(fall2023): Implement the commit logic!
  for (auto &write_set : txn->GetWriteSets()) {
    auto table_info = catalog_->GetTable(write_set.first);

    for (auto &rid : write_set.second) {
      auto prev_meta = table_info->table_->GetTupleMeta(rid);
      if (prev_meta.is_deleted_) {
        auto first_undo_link = GetUndoLink(rid);
        if (!first_undo_link.has_value() || !first_undo_link->IsValid()) {
          // this tuple is created by this transaction then deleted by this transaction
          prev_meta.ts_ = 0;  // set to 0 to avoid other txn to read this tuple
        } else {
          prev_meta.ts_ = commit_ts;
        }
      } else {
        prev_meta.ts_ = commit_ts;
      }
      table_info->table_->UpdateTupleMeta(prev_meta, rid);
    }
  }

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);

  // TODO(fall2023): set commit timestamp + update last committed timestamp here.
  txn->commit_ts_ = commit_ts;
  ++last_commit_ts_;

  txn->state_ = TransactionState::COMMITTED;
  running_txns_.UpdateCommitTs(txn->commit_ts_);
  running_txns_.RemoveTxn(txn->read_ts_);
  return true;
}

void TransactionManager::Abort(Transaction *txn) {
  if (txn->state_ != TransactionState::RUNNING && txn->state_ != TransactionState::TAINTED) {
    throw Exception("txn not in running / tainted state");
  }

  // TODO(fall2023): Implement the abort logic!

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  txn->state_ = TransactionState::ABORTED;
  running_txns_.RemoveTxn(txn->read_ts_);
}

void TransactionManager::GarbageCollection() {
  auto watermark = GetWatermark();
  std::unordered_set<txn_id_t> txns_can_be_accessed;

  for (auto &table_name : catalog_->GetTableNames()) {
    auto table_info = catalog_->GetTable(table_name);
    auto itr = table_info->table_->MakeEagerIterator();
    while (!itr.IsEnd()) {
      auto rid = itr.GetRID();
      auto meta = table_info->table_->GetTupleMeta(rid);
      if (meta.ts_ > watermark) {
        // table heap tuple is not visible to all transactions, we need to check the undo log
        auto undo_link = GetUndoLink(rid);
        while (undo_link.has_value() && undo_link->IsValid()) {
          txns_can_be_accessed.insert(undo_link->prev_txn_);
          auto undo_log = GetUndoLog(undo_link.value());
          if (undo_log.ts_ <= watermark) {
            // this is the deepest version that is visible to all transactions
            break;
          }
          undo_link = undo_log.prev_version_;
        }
      }  // meta.ts_ <= water_mark, table heap tuple is already visible to all transactions, undo log is not needed

      ++itr;
    }
  }

  std::vector<txn_id_t> txns_to_remove;
  for (auto &[txn_id, txn] : txn_map_) {
    if (txns_can_be_accessed.find(txn_id) == txns_can_be_accessed.end() &&
        (txn->state_ == TransactionState::COMMITTED || txn->state_ == TransactionState::ABORTED)) {
      txns_to_remove.push_back(txn_id);
    }
  }

  for (auto txn_id : txns_to_remove) {
    txn_map_.erase(txn_id);
  }
}

}  // namespace bustub

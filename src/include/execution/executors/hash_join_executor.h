//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <functional>
#include <memory>
#include <unordered_map>
#include <utility>

#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

struct HashJoinKey {
  std::vector<Value> keys_;

  auto operator==(const HashJoinKey &other) const -> bool {
    for (uint32_t i = 0; i < other.keys_.size(); i++) {
      if (keys_[i].CompareEquals(other.keys_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};

struct HashJoinLeftValueInfo {
  Tuple tuple_;
  bool has_matched_right_ = false;

  HashJoinLeftValueInfo(Tuple tuple, bool has_matched_right)
      : tuple_(std::move(tuple)), has_matched_right_(has_matched_right) {}
};
}  // namespace bustub

namespace std {
template <>
struct hash<bustub::HashJoinKey> {
  auto operator()(const bustub::HashJoinKey &join_key) const -> std::size_t {
    size_t curr_hash = 0;
    for (const auto &key : join_key.keys_) {
      if (!key.IsNull()) {
        curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&key));
      }
    }
    return curr_hash;
  }
};
}  // namespace std

namespace bustub {

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join.
   * @param[out] rid The next tuple RID, not used by hash join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  /** The HashJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;

  /** Left Child Executor */
  std::unique_ptr<AbstractExecutor> left_executor_;

  /** Right Child Executor */
  std::unique_ptr<AbstractExecutor> right_executor_;

  /**
   * Next() for inner join.
   */
  auto NextInnerJoin(Tuple *tuple, RID *rid) -> bool;

  /**
   * Next() for left join.
   */
  auto NextLeftJoin(Tuple *tuple, RID *rid) -> bool;

  /**
   * Make a hash join key from the left tuple.
   */
  auto MakeHashJoinKeyLeft(const Tuple *tuple) -> HashJoinKey;

  /**
   * Make a hash join key from the right tuple.
   */
  auto MakeHashJoinKeyRight(const Tuple *tuple) -> HashJoinKey;

  /**
   * The hash table.
   */
  std::unordered_map<HashJoinKey, std::vector<HashJoinLeftValueInfo>> hash_table_{};

  /**
   * The iterator for the hash table.
   */
  std::unordered_map<HashJoinKey, std::vector<HashJoinLeftValueInfo>>::iterator hash_table_iterator_;

  /**
   * The iterator for the value info list(A value info list is the 'value' of a entry in the hash table)
   */
  std::vector<HashJoinLeftValueInfo>::iterator value_info_list_iterator_;

  /**
   * The current matched right tuple.
   */
  Tuple cur_matched_right_tuple_;

  /**
   * Whether there is a matched right tuple.
   */
  bool matched_ = false;

  /**
   * Whether to start null matching(For left join)
   */
  bool start_null_matching_ = false;
};

}  // namespace bustub

#include "execution/executors/sort_executor.h"
#include "binder/bound_order_by.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_executor_->Init();

  std::vector<OrderByType> order_by_types;
  for (const auto &order_by_info : plan_->GetOrderBy()) {
    order_by_types.push_back(order_by_info.first);
  }

  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    SortEntry entry;
    entry.tuple_ = tuple;
    entry.rid_ = rid;

    for (const auto &order_by_info : plan_->GetOrderBy()) {
      entry.keys_.push_back(order_by_info.second->Evaluate(&entry.tuple_, child_executor_->GetOutputSchema()));
    }

    entries_.push_back(entry);
  }

  std::sort(entries_.begin(), entries_.end(), SortComparator(order_by_types));

  entry_itr_ = entries_.begin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (entry_itr_ == entries_.end()) {
    return false;
  }

  *tuple = entry_itr_->tuple_;
  *rid = entry_itr_->rid_;
  ++entry_itr_;
  return true;
}

}  // namespace bustub

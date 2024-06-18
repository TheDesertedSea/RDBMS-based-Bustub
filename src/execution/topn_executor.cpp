#include "execution/executors/topn_executor.h"
#include <memory>
#include <queue>

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)), top_entries_(nullptr) {}

void TopNExecutor::Init() {
  child_executor_->Init();

  std::vector<OrderByType> order_by_types;
  for (const auto &order_by_info : plan_->GetOrderBy()) {
    order_by_types.push_back(order_by_info.first);
  }

  top_entries_ = std::make_unique<std::priority_queue<TopNEntry, std::vector<TopNEntry>, TopNComparator>>();

  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    TopNEntry entry;
    entry.tuple_ = tuple;
    entry.order_by_types_ = order_by_types;
    entry.rid = rid;

    for (const auto &order_by_info : plan_->GetOrderBy()) {
      entry.keys_.push_back(order_by_info.second->Evaluate(&entry.tuple_, child_executor_->GetOutputSchema()));
    }

    top_entries_->push(entry);
    if (GetNumInHeap() > plan_->GetN()) {
      top_entries_->pop();
    }
  }

  sorted_top_entries_.resize(GetNumInHeap());
  for (size_t i = sorted_top_entries_.size(); i > 0; i--) {
    sorted_top_entries_[i - 1] = top_entries_->top();
    top_entries_->pop();
  }

  entry_itr_ = sorted_top_entries_.begin();
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (entry_itr_ == sorted_top_entries_.end()) {
    return false;
  }

  *tuple = entry_itr_->tuple_;
  *rid = entry_itr_->rid;
  ++entry_itr_;
  return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return top_entries_->size(); }

}  // namespace bustub

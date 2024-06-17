#include <iostream>
#include <memory>
#include "binder/tokens.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSeqScanAsIndexScan(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::SeqScan) {
    const auto &seq_scan_plan = dynamic_cast<const SeqScanPlanNode &>(*optimized_plan);

    // This project only requires optimize single equality predicate
    auto filter_predicate = std::dynamic_pointer_cast<ComparisonExpression>(seq_scan_plan.filter_predicate_);
    if (filter_predicate != nullptr && filter_predicate->comp_type_ == ComparisonType::Equal) {
      // Left child should be column value expression
      auto column_expr = std::dynamic_pointer_cast<ColumnValueExpression>(filter_predicate->GetChildAt(0));
      auto column_oid = column_expr->GetColIdx();
      auto indexes = catalog_.GetTableIndexes(catalog_.GetTable(seq_scan_plan.GetTableOid())->name_);
      for (const auto &index : indexes) {
        if (index->index_oid_ == column_oid) {
          return std::make_shared<IndexScanPlanNode>(
              seq_scan_plan.output_schema_, seq_scan_plan.GetTableOid(), index->index_oid_, nullptr,
              std::dynamic_pointer_cast<ConstantValueExpression>(filter_predicate->GetChildAt(1)).get());
        }
      }
    }
  }
  return plan;
}

}  // namespace bustub

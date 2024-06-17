#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

auto GetJoinColumnExpressions(const AbstractExpressionRef &expr)
    -> std::pair<std::vector<AbstractExpressionRef>, std::vector<AbstractExpressionRef>> {
  auto comp_expr = std::dynamic_pointer_cast<ComparisonExpression>(expr);
  if (comp_expr != nullptr) {
    auto column_expr_left = std::dynamic_pointer_cast<ColumnValueExpression>(comp_expr->GetChildAt(0));
    auto column_expr_right = std::dynamic_pointer_cast<ColumnValueExpression>(comp_expr->GetChildAt(1));
    BUSTUB_ASSERT(column_expr_left != nullptr && column_expr_right != nullptr,
                  "Children of comparison expression must be column value expressions");
    if (column_expr_left->GetTupleIdx() == 0) {
      return {{column_expr_left}, {column_expr_right}};
    }
    return {{column_expr_right}, {column_expr_left}};
  }

  auto logical_expr = std::dynamic_pointer_cast<LogicExpression>(expr);
  if (logical_expr == nullptr) {
    // No filter
    return {{}, {}};
  }

  auto left = GetJoinColumnExpressions(logical_expr->GetChildAt(0));
  auto right = GetJoinColumnExpressions(logical_expr->GetChildAt(1));

  std::vector<AbstractExpressionRef> left_join_columns;
  std::vector<AbstractExpressionRef> right_join_columns;

  left_join_columns.insert(left_join_columns.end(), left.first.begin(), left.first.end());
  left_join_columns.insert(left_join_columns.end(), right.first.begin(), right.first.end());

  right_join_columns.insert(right_join_columns.end(), left.second.begin(), left.second.end());
  right_join_columns.insert(right_join_columns.end(), right.second.begin(), right.second.end());

  return {left_join_columns, right_join_columns};
}

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Fall: You should support join keys of any number of conjunction of equi-condistions:
  // E.g. <column expr> = <column expr> AND <column expr> = <column expr> AND ...
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSeqScanAsIndexScan(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    auto join_columns = GetJoinColumnExpressions(nlj_plan.Predicate());

    // Also optimize the left and right children plans
    return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, OptimizeNLJAsHashJoin(nlj_plan.GetLeftPlan()),
                                              OptimizeNLJAsHashJoin(nlj_plan.GetRightPlan()), join_columns.first,
                                              join_columns.second, nlj_plan.GetJoinType());
  }
  if (optimized_plan->GetType() == PlanType::Projection) {
    // NestedLoopJoin may be under a projection
    auto projection_plan = dynamic_cast<const ProjectionPlanNode &>(*optimized_plan);
    return std::make_shared<ProjectionPlanNode>(projection_plan.output_schema_, projection_plan.expressions_,
                                                OptimizeNLJAsHashJoin(projection_plan.GetChildPlan()));
  }

  return plan;
}

}  // namespace bustub

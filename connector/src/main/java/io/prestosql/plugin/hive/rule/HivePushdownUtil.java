/*
 * Copyright (C) 2018-2021. Huawei Technologies Co., Ltd. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.plugin.hive.rule;

import com.google.common.collect.ImmutableSet;
import com.huawei.boostkit.omnidata.exception.OmniExpressionChecker;
import io.prestosql.expressions.DefaultRowExpressionTraversalVisitor;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.plugin.hive.HiveTableHandle;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.spi.type.Type;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.prestosql.plugin.hive.HiveColumnHandle.ColumnType.DUMMY_OFFLOADED;

public class HivePushdownUtil
{
    private HivePushdownUtil() {}

    public static Set<HiveColumnHandle> getDataSourceColumns(TableScanNode node)
    {
        ImmutableSet.Builder<HiveColumnHandle> builder = new ImmutableSet.Builder<>();
        for (Map.Entry<Symbol, ColumnHandle> entry : node.getAssignments().entrySet()) {
            HiveColumnHandle hiveColumnHandle = (HiveColumnHandle) entry.getValue();
            if (hiveColumnHandle.getColumnType().equals(DUMMY_OFFLOADED)) {
                continue;
            }
            builder.add(hiveColumnHandle);
        }

        return builder.build();
    }

    public static Set<VariableReferenceExpression> extractAll(RowExpression expression)
    {
        ImmutableSet.Builder<VariableReferenceExpression> builder = ImmutableSet.builder();
        expression.accept(new VariableReferenceBuilderVisitor(), builder);
        return builder.build();
    }

    public static class VariableReferenceBuilderVisitor
            extends DefaultRowExpressionTraversalVisitor<ImmutableSet.Builder<VariableReferenceExpression>>
    {
        @Override
        public Void visitVariableReference(VariableReferenceExpression variable, ImmutableSet.Builder<VariableReferenceExpression> builder)
        {
            builder.add(variable);
            return null;
        }
    }

    public static boolean isColumnsCanOffload(ConnectorTableHandle tableHandle, List<Symbol> outputSymbols, Map<String, Type> typesMap)
    {
        // just for performance, avoid query types
        if (tableHandle instanceof HiveTableHandle) {
            HiveTableHandle hiveTableHandle = (HiveTableHandle) tableHandle;
            if (hiveTableHandle.getOffloadExpression().isPresent()) {
                return true;
            }
        }

        for (Symbol symbol : outputSymbols) {
            Type type = typesMap.get(symbol.getName());
            if (!OmniExpressionChecker.checkType(type)) {
                return false;
            }
        }
        return true;
    }
}

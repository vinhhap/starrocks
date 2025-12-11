// Copyright 2021-present StarRocks, Inc. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.authorization;

import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.catalog.View;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.analyzer.Field;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectList;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Optimizer;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import com.starrocks.sql.optimizer.transformer.OptExprBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class ColumnPrivilegeExternalTest {
    private static class DummyExternalAccessController extends ExternalAccessController {
        // No-op implementation for testing; all default AccessController methods throw by design.
    }

    @Test
    public void testExternalControllerChecksViewObjectPrivilege() {
        TableName tableName = new TableName(null, "db1", "v1");
        TableRelation tableRelation = new TableRelation(tableName);

        Field columnField = new Field("c1", Type.INT, tableName, null);
        Column column = new Column("c1", Type.INT);
        HashMap<Field, Column> columns = new HashMap<>();
        columns.put(columnField, column);
        tableRelation.setColumns(columns);

        View view = Mockito.mock(View.class);
        Mockito.when(view.isConnectorView()).thenReturn(false);
        Mockito.when(view.isSecurity()).thenReturn(false);
        Mockito.when(view.isMaterializedView()).thenReturn(false);
        Mockito.when(view.getType()).thenReturn(Table.TableType.OLAP);
        Mockito.when(view.getName()).thenReturn("v1");

        tableRelation.setTable(view);

        SelectRelation selectRelation = new SelectRelation(new SelectList(), tableRelation, null, null, null);
        QueryStatement queryStatement = new QueryStatement((QueryRelation) selectRelation);

        AccessControlProvider provider = new AccessControlProvider(null, new DummyExternalAccessController());
        AtomicReference<TableName> checkedTable = new AtomicReference<>();

        try (MockedStatic<Authorizer> authorizer = Mockito.mockStatic(Authorizer.class)) {
            authorizer.when(Authorizer::getInstance).thenReturn(provider);
            authorizer.when(() -> Authorizer.checkViewAction(Mockito.any(), Mockito.any(), Mockito.any()))
                    .thenAnswer(invocation -> {
                        checkedTable.set(invocation.getArgument(1));
                        return null;
                    });

            ColumnPrivilege.check(new ConnectContext(), queryStatement, Collections.emptyList());
        }

        Assertions.assertNotNull(checkedTable.get());
        Assertions.assertEquals(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, checkedTable.get().getCatalog());
        Assertions.assertEquals("db1", checkedTable.get().getDb());
        Assertions.assertEquals("v1", checkedTable.get().getTbl());
    }

    @Test
    public void testExternalControllerChecksBaseTableColumns() {
        TableName tableName = new TableName(null, "db1", "t1");
        TableRelation tableRelation = new TableRelation(tableName);

        Field columnField = new Field("c1", Type.INT, tableName, null);
        Column column = new Column("c1", Type.INT);
        HashMap<Field, Column> columns = new HashMap<>();
        columns.put(columnField, column);
        tableRelation.setColumns(columns);

        Table table = Mockito.mock(Table.class);
        Mockito.when(table.getName()).thenReturn("t1");
        Mockito.when(table.isMaterializedView()).thenReturn(false);
        tableRelation.setTable(table);

        SelectRelation selectRelation = new SelectRelation(new SelectList(), tableRelation, null, null, null);
        QueryStatement queryStatement = new QueryStatement((QueryRelation) selectRelation);

        ColumnRefOperator columnRef = new ColumnRefOperator(1, Type.INT, "c1", true);
        LogicalPlan logicalPlan = new LogicalPlan(
                new OptExprBuilder(
                        new DummyLogicalScanOperator(table, columnRef, column), Collections.emptyList(), null),
                List.of(columnRef), Collections.emptyList());
        OptExpression optExpression = logicalPlan.getRoot();

        AccessControlProvider provider = new AccessControlProvider(null, new DummyExternalAccessController());
        AtomicReference<TableName> checkedTable = new AtomicReference<>();
        AtomicReference<String> checkedColumn = new AtomicReference<>();

        Optimizer optimizer = Mockito.mock(Optimizer.class);
        Mockito.when(optimizer.optimize(Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(optExpression);

        try (MockedConstruction<com.starrocks.sql.optimizer.transformer.RelationTransformer> transformer =
                Mockito.mockConstruction(com.starrocks.sql.optimizer.transformer.RelationTransformer.class,
                    (mock, context) -> Mockito.when(mock.transformWithSelectLimit(Mockito.any()))
                        .thenReturn(logicalPlan));
                MockedStatic<OptimizerFactory> optimizerFactory = Mockito.mockStatic(OptimizerFactory.class);
                MockedStatic<Authorizer> authorizer = Mockito.mockStatic(Authorizer.class)) {

            optimizerFactory.when(() -> OptimizerFactory.create(Mockito.any())).thenReturn(optimizer);

            authorizer.when(Authorizer::getInstance).thenReturn(provider);
            authorizer.when(() -> Authorizer.checkTableAction(Mockito.any(), Mockito.anyString(), Mockito.anyString(),
                    Mockito.anyString(), Mockito.any())).thenAnswer(invocation -> {
                        checkedTable.set(new TableName((String) invocation.getArgument(1),
                                invocation.getArgument(2), invocation.getArgument(3)));
                        return null;
                    });
            authorizer.when(() -> Authorizer.checkColumnAction(Mockito.any(), Mockito.any(), Mockito.anyString(),
                    Mockito.any())).thenAnswer(invocation -> {
                        checkedColumn.set(invocation.getArgument(2));
                        return null;
                    });

            ColumnPrivilege.check(new ConnectContext(), queryStatement, Collections.emptyList());
        }

        Assertions.assertNotNull(checkedTable.get());
        Assertions.assertEquals(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, checkedTable.get().getCatalog());
        Assertions.assertEquals("db1", checkedTable.get().getDb());
        Assertions.assertEquals("t1", checkedTable.get().getTbl());
        Assertions.assertEquals("c1", checkedColumn.get());
    }

    private static class DummyLogicalScanOperator extends LogicalScanOperator {
        DummyLogicalScanOperator(Table table, ColumnRefOperator columnRef, Column column) {
            super(OperatorType.LOGICAL_OLAP_SCAN, table,
                    Collections.singletonMap(columnRef, column),
                    Collections.singletonMap(column, columnRef),
                    Operator.DEFAULT_LIMIT, null, null);
        }

        @Override
        public <R, C> R accept(com.starrocks.sql.optimizer.OptExpressionVisitor<R, C> visitor,
                                com.starrocks.sql.optimizer.OptExpression optExpression, C context) {
            return visitor.visitLogicalTableScan(optExpression, context);
        }
    }
}

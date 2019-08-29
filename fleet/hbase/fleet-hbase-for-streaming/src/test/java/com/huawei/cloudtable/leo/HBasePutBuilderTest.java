package com.huawei.cloudtable.leo;

import com.huawei.cloudtable.leo.expression.Constant;
import com.huawei.cloudtable.leo.expression.Evaluation;
import com.huawei.cloudtable.leo.metadata.TableDefinition;
import org.apache.hadoop.hbase.client.Put;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class HBasePutBuilderTest {

  @Test
  public void testBuild() {
    final HBaseTableReference tableReference = getTestTableReference();// 元数据对象
    final TableDefinition.Column<?>[] columns = new TableDefinition.Column<?>[]{
        tableReference.getColumn(Identifier.of("name")),
        tableReference.getColumn(Identifier.of("age"))
    };
    final Evaluation<?>[] columnValues = new Evaluation<?>[]{
        new Constant<>(String.class, "Zhang San"),
        new Constant<>(Integer.class, 20)
    };
    final Put put = HBasePutBuilder.build(tableReference, columns, columnValues);
    System.out.println(put);
  }

  private static final HBaseIdentifier DEFAULT_FAMILY = HBaseIdentifier.of("f");

  private static HBaseTableReference getTestTableReference() {
    final TableDefinition.Builder tableDefinitionBuilder = new TableDefinition.Builder(Identifier.of("test"));
    final TableDefinition.Column<?> nameColumn = tableDefinitionBuilder.addColumn(
        Identifier.of("name"),
        ValueTypeManager.BUILD_IN.getValueType(String.class),
        false,
        null,
        null
    );
    final TableDefinition.Column<?> ageColumn = tableDefinitionBuilder.addColumn(
        Identifier.of("age"),
        ValueTypeManager.BUILD_IN.getValueType(Integer.class),
        false,
        null,
        null
    );
    tableDefinitionBuilder.setPrimaryKey(Identifier.of("PK"), Identifier.of("name"));
    final TableDefinition tableDefinition = tableDefinitionBuilder.build();
    final List<HBaseTableColumnReference<?>> tableColumnReferences = new ArrayList<>(tableDefinition.getColumnCount());
    tableColumnReferences.add(new HBaseTableColumnReference(nameColumn, HBaseValueCodecManager.BUILD_IN.getValueCodec(nameColumn.getValueClass())));
    tableColumnReferences.add(new HBaseTableColumnReference(ageColumn, HBaseValueCodecManager.BUILD_IN.getValueCodec(ageColumn.getValueClass()), DEFAULT_FAMILY, HBaseIdentifier.of(ageColumn.getName().toString())));
    HBaseTableColumnReference rowMarkColumn = tableColumnReferences.get(1);
    return new HBaseTableReference(
        "",
        HBaseIdentifier.of("test"),
        tableDefinition,
        tableColumnReferences,
        HBaseRowKeyCodecType.DELIMITER,
        rowMarkColumn.getFamily(),
        rowMarkColumn.getQualifier()
    );
  }

}

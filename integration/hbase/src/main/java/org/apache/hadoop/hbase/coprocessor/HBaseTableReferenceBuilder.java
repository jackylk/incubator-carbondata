package org.apache.hadoop.hbase.coprocessor;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.apache.carbondata.sdk.file.Field;

import com.huawei.cloudtable.leo.HBaseIdentifier;
import com.huawei.cloudtable.leo.HBaseRowKeyCodecType;
import com.huawei.cloudtable.leo.HBaseTableColumnReference;
import com.huawei.cloudtable.leo.HBaseTableReference;
import com.huawei.cloudtable.leo.HBaseValueCodecManager;
import com.huawei.cloudtable.leo.Identifier;
import com.huawei.cloudtable.leo.SparkHBaseTypeConverter;
import com.huawei.cloudtable.leo.ValueTypeManager;
import com.huawei.cloudtable.leo.metadata.TableDefinition;
import com.huawei.cloudtable.leo.value.Date;
import com.huawei.cloudtable.leo.value.Time;
import com.huawei.cloudtable.leo.value.Timestamp;

public class HBaseTableReferenceBuilder {
  private static final HBaseIdentifier DEFAULT_FAMILY = HBaseIdentifier.of("cf");
  private static final HBaseIdentifier DEFAULT_QUALIFIER = HBaseIdentifier.of(" ");

  public static HBaseTableReference buildTableReference(String tableName, String primaryKeys,
      Field[] fields) {
    final TableDefinition.Builder tableDefinitionBuilder =
        new TableDefinition.Builder(new Identifier(tableName));
    for (int index = 0; index < fields.length; index++) {
      tableDefinitionBuilder.addColumn(Identifier.of(fields[index].getFieldName()),
          ValueTypeManager.BUILD_IN.getValueType(
              SparkHBaseTypeConverter.toValueClass(fields[index].getDataType().getName())),
          false, null, null);
    }
    if (primaryKeys != null) {
      final String[] primaryKeyColumnNames = primaryKeys.trim().split(",");
      final List<Identifier> columnNameList = new ArrayList<>(primaryKeyColumnNames.length);
      for (int index = 0; index < primaryKeyColumnNames.length; index++) {
        columnNameList.add(Identifier.of(primaryKeyColumnNames[index]));
      }
      tableDefinitionBuilder.setPrimaryKey(Identifier.of("PK"), columnNameList);
    }
    final TableDefinition tableDefinition = tableDefinitionBuilder.build();
    final List<HBaseTableColumnReference<?>> columnReferenceList =
        new ArrayList<>(tableDefinition.getColumnCount());
    for (int index = 0; index < tableDefinition.getColumnCount(); index++) {
      final TableDefinition.Column<?> column = tableDefinition.getColumn(index);
      if (tableDefinition.getPrimaryKey() != null
          && tableDefinition.getPrimaryKey().getColumnIndex(column) != null) {
        columnReferenceList.add(new HBaseTableColumnReference(column,
            HBaseValueCodecManager.BUILD_IN.getValueCodec(column.getValueClass())));
      } else {
        columnReferenceList.add(new HBaseTableColumnReference(column,
            HBaseValueCodecManager.BUILD_IN.getValueCodec(column.getValueClass()), DEFAULT_FAMILY,
            HBaseIdentifier.of(column.getName().toString())));
      }
    }
    return new HBaseTableReference("", HBaseIdentifier.of("fake"), tableDefinition,
        columnReferenceList, HBaseRowKeyCodecType.DELIMITER, DEFAULT_FAMILY, DEFAULT_QUALIFIER);
  }

}

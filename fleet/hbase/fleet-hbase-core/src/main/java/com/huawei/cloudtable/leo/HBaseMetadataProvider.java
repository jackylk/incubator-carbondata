
package com.huawei.cloudtable.leo;

public abstract class HBaseMetadataProvider {

    public abstract HBaseTableReference getTableReference(String tenantIdentifier, Identifier schemaName,
        Identifier tableName);

}

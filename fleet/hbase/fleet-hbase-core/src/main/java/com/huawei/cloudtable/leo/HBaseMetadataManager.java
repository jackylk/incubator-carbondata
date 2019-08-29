
package com.huawei.cloudtable.leo;

import java.io.Closeable;

public class HBaseMetadataManager implements Closeable {

    public HBaseMetadataManager(final HBaseMetadataProvider metadataProvider) {
        if (metadataProvider == null) {
            throw new IllegalArgumentException("Argument [metadataProvider] is null.");
        }
        this.metadataProvider = metadataProvider;
        this.cache = new HBaseMetadataCache();
    }

    private final HBaseMetadataProvider metadataProvider;

    private final HBaseMetadataCache cache;

    public HBaseTableReference getTableReference(final String tenantIdentifier, final Identifier schemaName,
        final Identifier tableName) {
        final HBaseMetadataCache.Key cacheKey =
            new HBaseMetadataCache.Key(tenantIdentifier, schemaName.toString(), tableName.toString());
        HBaseTableReference tableReference = this.getTableReferenceFromCache(cacheKey);
        if (tableReference == null) {
            tableReference = this.getTableReferenceFromPersistent(tenantIdentifier, schemaName, tableName);
            if (tableReference == null) {
                return null;
            } else {
                this.putTableReferenceToCache(cacheKey, tableReference);
            }
        }
        return tableReference;
    }

    private HBaseTableReference getTableReferenceFromCache(final HBaseMetadataCache.Key cacheKey) {
        return (HBaseTableReference) this.cache.getIfPresent(cacheKey);
    }

    @SuppressWarnings("unchecked")
    private HBaseTableReference getTableReferenceFromPersistent(final String tenantIdentifier,
                                                                final Identifier schemaName, final Identifier tableName) {
        return this.metadataProvider.getTableReference(tenantIdentifier, schemaName, tableName);
    }

    private void putTableReferenceToCache(final HBaseMetadataCache.Key cacheKey,
        final HBaseTableReference tableReference) {
        this.cache.put(cacheKey, tableReference);
    }

    @Override
    public void close() {
        // to do nothing.
    }

}

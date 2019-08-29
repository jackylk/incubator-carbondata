package com.huawei.cloudtable.leo;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.hadoop.conf.Configuration;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

public final class HBaseMetadataCache {

  public HBaseMetadataCache() {
    this.cache = CacheBuilder.newBuilder().build();
  }

  private final Cache<Key, Object> cache;

  @Nullable
  public Object getIfPresent(final Key key) {
    return this.cache.getIfPresent(key);
  }

  public Object get(final Key key, final Callable<?> callable) throws ExecutionException {
    return this.cache.get(key, callable);
  }

  public void put(final Key key, final Object object) {
    this.cache.put(key, object);
  }

  public void invalidate(final Key key) {
    this.cache.invalidate(key);
  }

  public void invalidateAll() {
    this.cache.invalidateAll();
  }

  public long size() {
    return this.cache.size();
  }

  public void cleanUp() {
    this.cache.cleanUp();
  }

  public static final class Key {

    public Key(
        final String tenantIdentifier,
        final String schemaName,
        final String elementName
    ) {
      this(tenantIdentifier, schemaName, elementName, null);
    }

    public Key(
        final String tenantIdentifier,
        final String schemaName,
        final String elementName,
        final String columnName
    ) {
      if (tenantIdentifier == null) {
        throw new IllegalArgumentException("Argument [tenantIdentifier] is null.");
      }
      if (schemaName == null) {
        throw new IllegalArgumentException("Argument [schemaName] is null.");
      }
      if (elementName == null) {
        throw new IllegalArgumentException("Argument [elementName] is null.");
      }
      this.tenantIdentifier = tenantIdentifier;
      this.schemaName = schemaName;
      this.elementName = elementName;
      this.columnName = columnName;
    }

    private final String tenantIdentifier;

    private final String schemaName;

    private final String elementName;

    private final String columnName;

    public String getTenantIdentifier() {
      return this.tenantIdentifier;
    }

    public String getSchemaName() {
      return this.schemaName;
    }

    public String getElementName() {
      return this.elementName;
    }

    public String getColumnName() {
      return this.columnName;
    }

    @Override
    public boolean equals(final Object object) {
      if (object == this) {
        return true;
      }
      if (object == null || object.getClass() != this.getClass()) {
        return false;
      }
      final Key that = (Key) object;
      return Objects.equals(this.tenantIdentifier, that.tenantIdentifier) &&
          Objects.equals(this.schemaName, that.schemaName) &&
          Objects.equals(this.elementName, that.elementName) &&
          Objects.equals(this.columnName, that.columnName);
    }

    @Override
    public int hashCode() {
      return Objects.hash(this.tenantIdentifier, this.schemaName, this.elementName, this.columnName);
    }

  }

}

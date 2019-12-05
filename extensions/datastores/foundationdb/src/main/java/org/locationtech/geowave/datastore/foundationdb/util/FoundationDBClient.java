package org.locationtech.geowave.datastore.foundationdb.util;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.NetworkOptions;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.subspace.Subspace;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import java.io.Closeable;
import java.util.Arrays;
import java.util.Objects;
import org.locationtech.geowave.core.store.operations.MetadataType;
import org.locationtech.geowave.datastore.foundationdb.FoundationDBFactoryHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.io.File;
import java.util.Arrays;
import java.util.Objects;
import org.locationtech.geowave.core.store.operations.MetadataType;
import org.locationtech.geowave.datastore.foundationdb.FoundationDBFactoryHelper;
import java.util.Objects;

public class FoundationDBClient implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(FoundationDBClient.class);

  private final Cache<Subspace, CacheKey> keyCache = Caffeine.newBuilder().build();
  private final LoadingCache<IndexCacheKey, FoundationDBIndexTable> indexTableCache =
      Caffeine.newBuilder().build(this::loadIndexTable);

  private final LoadingCache<DataIndexCacheKey, FoundationDBDataIndexTable> dataIndexTableCache =
      Caffeine.newBuilder().build(this::loadDataIndexTable);
  private final LoadingCache<CacheKey, FoundationDBMetadataTable> metadataTableCache =
      Caffeine.newBuilder().build(this::loadMetadataTable);
  private final FoundationDBFactoryHelper factoryHelper = new FoundationDBFactoryHelper();

  private final boolean visibilityEnabled;
  private final int batchWriteSize;
  private final FDB fdb;
  private Subspace subDirectorySubspace;

  public FoundationDBClient(
      final String subDirectory,
      final boolean visibilityEnabled,
      final int batchWriteSize) {
    LOGGER.warn("SUBDIR: " + subDirectory);
    this.visibilityEnabled = visibilityEnabled;
    this.batchWriteSize = batchWriteSize;
    this.fdb = FDB.selectAPIVersion(610);
    this.subDirectorySubspace = new Subspace(Tuple.from(subDirectory).pack());
  }

  private static class CacheKey {
    protected final Subspace directorySubspace;
    protected final boolean requiresTimestamp;

    public CacheKey(final Subspace directorySubspace, final boolean requiresTimestamp) {
      this.directorySubspace = directorySubspace;
      this.requiresTimestamp = requiresTimestamp;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = (prime * result) + ((directorySubspace == null) ? 0 : directorySubspace.hashCode());
      return result;
    }

    @Override
    public boolean equals(final Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      final CacheKey other = (CacheKey) obj;
      if (directorySubspace == null) {
        if (other.directorySubspace != null) {
          return false;
        }
      } else if (!directorySubspace.equals(other.directorySubspace)) {
        return false;
      }
      return true;
    }
  }

  private static class IndexCacheKey extends DataIndexCacheKey {
    protected final byte[] partition;

    public IndexCacheKey(
        final Subspace directorySubspace,
        final short adapterId,
        final byte[] partition,
        final boolean requiresTimestamp) {
      super(directorySubspace, requiresTimestamp, adapterId);
      this.partition = partition;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = super.hashCode();
      result = (prime * result) + adapterId;
      result = (prime * result) + Arrays.hashCode(partition);
      return result;
    }

    @Override
    public boolean equals(final Object obj) {
      if (this == obj) {
        return true;
      }
      if (!super.equals(obj)) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      final IndexCacheKey other = (IndexCacheKey) obj;
      if (adapterId != other.adapterId) {
        return false;
      }
      if (!Arrays.equals(partition, other.partition)) {
        return false;
      }
      return true;
    }
  }
  private static class DataIndexCacheKey extends CacheKey {
    protected final short adapterId;

    public DataIndexCacheKey(final Subspace directorySubspace, final short adapterId) {
      super(directorySubspace, false);
      this.adapterId = adapterId;
    }

    private DataIndexCacheKey(
        final Subspace directorySubspace,
        final boolean requiresTimestamp,
        final short adapterId) {
      super(directorySubspace, requiresTimestamp);
      this.adapterId = adapterId;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = super.hashCode();
      result = (prime * result) + adapterId;
      return result;
    }

    @Override
    public boolean equals(final Object obj) {
      if (this == obj) {
        return true;
      }
      if (!super.equals(obj)) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      final IndexCacheKey other = (IndexCacheKey) obj;
      if (adapterId != other.adapterId) {
        return false;
      }
      return true;
    }
  }

  private FoundationDBIndexTable loadIndexTable(final IndexCacheKey key) {
    return new FoundationDBIndexTable(
        key.adapterId,
        key.partition,
        key.requiresTimestamp,
        visibilityEnabled,
        batchWriteSize,
        this.fdb.open());
  }

  private FoundationDBDataIndexTable loadDataIndexTable(final DataIndexCacheKey key) {
    return new FoundationDBDataIndexTable(key.adapterId, visibilityEnabled, batchWriteSize, this.fdb.open());
  }

  private FoundationDBMetadataTable loadMetadataTable(final CacheKey key) {
    return new FoundationDBMetadataTable(this.fdb.open(), key.requiresTimestamp, visibilityEnabled);
  }

  public synchronized FoundationDBIndexTable getIndexTable(
      final String tableName,
      final short adapterId,
      final byte[] partition,
      final boolean requiresTimestamp) {
    final Subspace directorySubspace = subDirectorySubspace.get(Tuple.from(tableName).pack());
    return indexTableCache.get(
        (IndexCacheKey) Objects.requireNonNull(
            keyCache.get(
                directorySubspace,
                d -> new IndexCacheKey(d, adapterId, partition, requiresTimestamp))));
  }

  public boolean indexTableExists(final String indexName) {
    // then look for prefixes of this index directory in which case there is
    // a partition key
    for (final Subspace key : keyCache.asMap().keySet()) {
      if (key.contains(Tuple.from(indexName).pack())) {
        return true;
      }
    }
    return false;
  }


  public synchronized FoundationDBMetadataTable getMetadataTable(final MetadataType type) {
    final Subspace directorySubspace = subDirectorySubspace.get(Tuple.from(type.name()).pack());
    return metadataTableCache.get(
        Objects.requireNonNull(
            keyCache.get(
                directorySubspace,
                d -> new CacheKey(d, type.equals(MetadataType.STATS)))));
  }

  public boolean metadataTableExists(final MetadataType type) {
    // this could have been created by a different process so check the
    // directory listing
    final Subspace directorySubspace = subDirectorySubspace.get(Tuple.from(type.name()).pack());
    return keyCache.getIfPresent(directorySubspace) != null;
  }

  public synchronized FoundationDBDataIndexTable getDataIndexTable(
      final String tableName,
      final short adapterId) {
    final Subspace directorySubspace = subDirectorySubspace.get(Tuple.from(tableName).pack());
    return dataIndexTableCache.get(
        (DataIndexCacheKey) Objects.requireNonNull(
            keyCache.get(directorySubspace, d -> new DataIndexCacheKey(d, adapterId))));
  }

  protected static NetworkOptions indexWriteOptions = null;

  public boolean isVisibilityEnabled() {
    return visibilityEnabled;
  }

  public Subspace getSubDirectorySubspace() {
    return subDirectorySubspace;
  }

  public void close() {
    keyCache.invalidateAll();
    indexTableCache.asMap().values().forEach(db -> db.close());
    indexTableCache.invalidateAll();
    dataIndexTableCache.asMap().values().forEach(db -> db.close());
    dataIndexTableCache.invalidateAll();
    metadataTableCache.asMap().values().forEach(db -> db.close());
    metadataTableCache.invalidateAll();
  }

}

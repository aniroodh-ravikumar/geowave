package org.locationtech.geowave.datastore.foundationdb.util;

import com.apple.foundationdb.NetworkOptions;
import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.locationtech.geowave.core.store.operations.MetadataType;
import org.locationtech.geowave.datastore.foundationdb.FoundationDBFactoryHelper;
import org.locationtech.geowave.datastore.foundationdb.operations.FoundationDBOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.io.File;
import java.util.Arrays;
import java.util.Objects;

public class FoundationDBClient implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(FoundationDBClient.class);

  private final Cache<String, CacheKey> keyCache = Caffeine.newBuilder().build();
  private final LoadingCache<IndexCacheKey, FoundationDBIndexTable> indexTableCache =
      Caffeine.newBuilder().build(this::loadIndexTable);

  private final LoadingCache<DataIndexCacheKey, FoundationDBDataIndexTable> dataIndexTableCache =
      Caffeine.newBuilder().build(this::loadDataIndexTable);
  private final LoadingCache<CacheKey, FoundationDBMetadataTable> metadataTableCache =
      Caffeine.newBuilder().build(this::loadMetadataTable);
  private final FoundationDBFactoryHelper factoryHelper = new FoundationDBFactoryHelper();

  private final String subDirectory;
  private final boolean visibilityEnabled;
  private final boolean compactOnWrite;
  private final int batchWriteSize;

  public FoundationDBClient(
      final String subDirectory,
      final boolean visibilityEnabled,
      final boolean compactOnWrite,
      final int batchWriteSize) {
    this.subDirectory = subDirectory;
    this.visibilityEnabled = visibilityEnabled;
    this.compactOnWrite = compactOnWrite;
    this.batchWriteSize = batchWriteSize;
  }

  private static class CacheKey {
    protected final String directory;
    protected final boolean requiresTimestamp;

    public CacheKey(final String directory, final boolean requiresTimestamp) {
      this.directory = directory;
      this.requiresTimestamp = requiresTimestamp;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = (prime * result) + ((directory == null) ? 0 : directory.hashCode());
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
      if (directory == null) {
        if (other.directory != null) {
          return false;
        }
      } else if (!directory.equals(other.directory)) {
        return false;
      }
      return true;
    }
  }

  private static class IndexCacheKey extends DataIndexCacheKey {
    protected final byte[] partition;

    public IndexCacheKey(
        final String directory,
        final short adapterId,
        final byte[] partition,
        final boolean requiresTimestamp) {
      super(directory, requiresTimestamp, adapterId);
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

    public DataIndexCacheKey(final String directory, final short adapterId) {
      super(directory, false);
      this.adapterId = adapterId;
    }

    private DataIndexCacheKey(
        final String directory,
        final boolean requiresTimestamp,
        final short adapterId) {
      super(directory, requiresTimestamp);
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
        compactOnWrite,
        batchWriteSize);
  }

  private FoundationDBDataIndexTable loadDataIndexTable(final DataIndexCacheKey key) {
    return new FoundationDBDataIndexTable(
        key.adapterId,
        visibilityEnabled,
        compactOnWrite,
        batchWriteSize);
  }

  private FoundationDBMetadataTable loadMetadataTable(final CacheKey key) {
    final File dir = new File(key.directory);
    if (!dir.exists() && !dir.mkdirs()) {
      LOGGER.error("Unable to create directory for foundationdb store '" + key.directory + "'");
    }
    FDB fdb = FDB.selectAPIVersion(610);
    Database db = fdb.open();
    return new FoundationDBMetadataTable(db, key.requiresTimestamp, visibilityEnabled);
  }

  public synchronized FoundationDBIndexTable getIndexTable(
      final String tableName,
      final short adapterId,
      final byte[] partition,
      final boolean requiresTimestamp) {
    final String directory = subDirectory + "/" + tableName;
    return indexTableCache.get(
        (IndexCacheKey) Objects.requireNonNull(
            keyCache.get(
                directory,
                d -> new IndexCacheKey(d, adapterId, partition, requiresTimestamp))));
  }

  public boolean indexTableExists(final String indexName) {
    // then look for prefixes of this index directory in which case there is
    // a partition key
    for (final String key : keyCache.asMap().keySet()) {
      if (key.substring(subDirectory.length()).contains(indexName)) {
        return true;
      }
    }
    // this could have been created by a different process so check the
    // directory listing
    final String[] listing = new File(subDirectory).list((dir, name) -> name.contains(indexName));
    return (listing != null) && (listing.length > 0);
  }


  public synchronized FoundationDBMetadataTable getMetadataTable(final MetadataType type) {
    final String directory = subDirectory + "/" + type.name();
    return metadataTableCache.get(
        Objects.requireNonNull(
            keyCache.get(directory, d -> new CacheKey(d, type.equals(MetadataType.STATS)))));
  }

  public boolean metadataTableExists(final MetadataType type) {
    // this could have been created by a different process so check the
    // directory listing
    return (keyCache.getIfPresent(subDirectory + "/" + type.name()) != null)
        || new File(subDirectory + "/" + type.name()).exists();
  }

  public synchronized FoundationDBDataIndexTable getDataIndexTable(
      final String tableName,
      final short adapterId) {
    final String directory = subDirectory + "/" + tableName;
    return dataIndexTableCache.get(
        (DataIndexCacheKey) Objects.requireNonNull(
            keyCache.get(directory, d -> new DataIndexCacheKey(d, adapterId))));
  }

  protected static NetworkOptions indexWriteOptions = null;

  public boolean isCompactOnWrite() {
    return compactOnWrite;
  }

  public boolean isVisibilityEnabled() {
    return visibilityEnabled;
  }

  public String getSubDirectory() {
    return subDirectory;
  }

  public void close() {}

}

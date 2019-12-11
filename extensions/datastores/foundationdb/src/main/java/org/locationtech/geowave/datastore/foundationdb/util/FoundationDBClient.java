package org.locationtech.geowave.datastore.foundationdb.util;

import com.apple.foundationdb.FDB;
import com.apple.foundationdb.NetworkOptions;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
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
import java.util.Arrays;
import java.util.Map;
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
  private String subDirectory;

  public FoundationDBClient(
      final String subDirectory,
      final boolean visibilityEnabled,
      final int batchWriteSize) {
    this.subDirectory = subDirectory;
    LOGGER.warn("SUBDIR: " + subDirectory);
    this.visibilityEnabled = visibilityEnabled;
    this.batchWriteSize = batchWriteSize;
    this.fdb = FDB.selectAPIVersion(610);
    this.subDirectorySubspace = new Subspace(Tuple.from(subDirectory).pack());
  }

  private static class CacheKey {
    // https://apple.github.io/foundationdb/javadoc/com/apple/foundationdb/subspace/Subspace.html
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

  /**
   * loads the indexTable associated with the foundationDBClient object.
   * 
   * @param key The IndexCacheKey object that is used to create a new FoundationDBIndexTable object.
   * @return A new FoundationDBIndexTable with the parameters associated with the key.
   */
  private FoundationDBIndexTable loadIndexTable(final IndexCacheKey key) {
    return new FoundationDBIndexTable(
        key.adapterId,
        key.partition,
        key.requiresTimestamp,
        visibilityEnabled,
        batchWriteSize,
        this.fdb.open());
  }

  /**
   * loads the dataIndexTable associated with the foundationDBClient object.
   * 
   * @param key The dataIndexCacheKey object that is used to create a new FoundationDBDataIndexTable
   *        object.
   * @return A new FoundationDBDataIndexTable with the parameters associated with the key.
   */
  private FoundationDBDataIndexTable loadDataIndexTable(final DataIndexCacheKey key) {
    return new FoundationDBDataIndexTable(
        key.adapterId,
        visibilityEnabled,
        batchWriteSize,
        this.fdb.open());
  }

  /**
   * loads the metadataTable associated with the foundationDBClient object.
   * 
   * @param key The CacheKey object that is used to create a new FoundationDBMetadataTable object.
   * @return A new FoundationDBMetadataTable with the parameters associated with the key.
   */
  private FoundationDBMetadataTable loadMetadataTable(final CacheKey key) {
    return new FoundationDBMetadataTable(this.fdb.open(), key.requiresTimestamp, visibilityEnabled);
  }

  /**
   * gets the indexTable from the indexTableCache field of the foundationDBClient.
   * 
   * @param tableName The name of the indexTable to be obtained.
   * @param adapterId The adapterId used to create the indexCacheKey to obtain the indexTable.
   * @param partition The partition used to create the indexCacheKey to obtain * the indexTable.
   * @param requiresTimestamp A boolean that represents whether or not a timestamp is to be included
   *        to create the indexCacheKey to obtain the indexTable.
   * @return An indexTable object with the given parameters.
   */
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

  /**
   * Checks if the indexTable of the given name exists in the cache.
   * 
   * @param indexName The name of the indexTable to check in the keyCache.
   * @return True if a key in the keyCache contains a tuple with a prefix of indexName.
   */
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

  /**
   * Gets the metadataTable from the cache for the given metaDataType.
   * 
   * @param type The type of the FoundationDBMetaDataTable object to be obtained.
   * @return the required FoundationDBMetaDataTable object.
   */
  public synchronized FoundationDBMetadataTable getMetadataTable(final MetadataType type) {
    final Subspace directorySubspace = subDirectorySubspace.get(Tuple.from(type.name()).pack());
    return metadataTableCache.get(
        Objects.requireNonNull(
            keyCache.get(
                directorySubspace,
                d -> new CacheKey(d, type.equals(MetadataType.STATS)))));
  }

  /**
   * Checks if the metaddataTable of the given type exists in the cache.
   * 
   * @param type The type of the metadataTable to check in the keyCache.
   * @return True if the keyCache contains the subspace given by the prefix type.
   */
  public boolean metadataTableExists(final MetadataType type) {
    // this could have been created by a different process so check the
    // directory listing
    final Subspace directorySubspace = subDirectorySubspace.get(Tuple.from(type.name()).pack());
    return keyCache.getIfPresent(directorySubspace) != null;
  }

  /**
   * Gets the dataIndexTable from the cache.
   * 
   * @param tableName The name of the dataIndexTable to be obtained.
   * @param adapterId The adaptedId associated with the cacheKey for the dataIndexTableCache
   * @return The required FoundationDBDataIndexTable object.
   */
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

  /**
   * Calls the invalidateAll method for all of the cache fields of the FoundationDBClient object,
   * and also closes all of the databases that are values in each of the caches.
   */
  public void close() {
    keyCache.invalidateAll();
    indexTableCache.asMap().values().forEach(db -> db.close());
    indexTableCache.invalidateAll();
    dataIndexTableCache.asMap().values().forEach(db -> db.close());
    dataIndexTableCache.invalidateAll();
    metadataTableCache.asMap().values().forEach(db -> db.close());
    metadataTableCache.invalidateAll();
  }

  public void close(final String indexName, final String typeName) {
    final String prefix = FoundationDBUtils.getTablePrefix(typeName, indexName);
    for (final Map.Entry<Subspace, CacheKey> e : keyCache.asMap().entrySet()) {
      final Subspace key = e.getKey();
      if (key.contains(prefix.getBytes())) {
        keyCache.invalidate(key);
        AbstractFoundationDBTable indexTable = indexTableCache.getIfPresent(e.getValue());
        if (indexTable == null) {
          indexTable = dataIndexTableCache.getIfPresent(e.getValue());
        }
        if (indexTable != null) {
          indexTableCache.invalidate(e.getValue());
          dataIndexTableCache.invalidate(e.getValue());
          indexTable.close();
        }
      }
    }
  }

}

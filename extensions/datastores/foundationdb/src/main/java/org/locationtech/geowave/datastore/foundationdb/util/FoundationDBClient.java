package org.locationtech.geowave.datastore.foundationdb.util;

import java.io.Closeable;
import java.util.Arrays;
import com.apple.foundationdb.NetworkOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FoundationDBClient implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(FoundationDBClient.class);

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

  //TODO: Implement this function
  public synchronized FoundationDBIndexTable getIndexTable(
          final String tableName,
          final short adapterId,
          final byte[] partition,
          final boolean requiresTimestamp) {

//    if (indexWriteOptions == null) {
////      FDB.loadLibrary();
//      final int cores = Runtime.getRuntime().availableProcessors();
//      indexWriteOptions =
//              new Options().setCreateIfMissing(true).prepareForBulkLoad().setIncreaseParallelism(cores);
//      indexReadOptions = new Options().setIncreaseParallelism(cores);
//      batchWriteOptions =
//              new WriteOptions().setDisableWAL(false).setNoSlowdown(false).setSync(false);
//    }
//    final String directory = subDirectory + "/" + tableName;
//    return indexTableCache.get(
//            (IndexCacheKey) keyCache.get(
//                    directory,
//                    d -> new IndexCacheKey(d, adapterId, partition, requiresTimestamp)));
    return null;
  }

  //TODO: Implement this function too.
  public synchronized FoundationDBDataIndexTable getDataIndexTable(
          final String tableName,
          final short adapterId) {
//    if (indexWriteOptions == null) {
//      FDB.loadLibrary();
//      final int cores = Runtime.getRuntime().availableProcessors();
//      indexWriteOptions =
//              new Options().setCreateIfMissing(true).prepareForBulkLoad().setIncreaseParallelism(cores);
//      indexReadOptions = new Options().setIncreaseParallelism(cores);
//      batchWriteOptions =
//              new WriteOptions().setDisableWAL(false).setNoSlowdown(false).setSync(false);
//    }
//    final String directory = subDirectory + "/" + tableName;
//    return dataIndexTableCache.get(
//            (DataIndexCacheKey) keyCache.get(directory, d -> new DataIndexCacheKey(d, adapterId)));
    return null;
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

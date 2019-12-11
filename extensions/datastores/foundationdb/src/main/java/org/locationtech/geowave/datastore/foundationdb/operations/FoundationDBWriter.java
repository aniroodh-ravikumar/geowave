package org.locationtech.geowave.datastore.foundationdb.operations;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.core.store.operations.RowWriter;
import org.locationtech.geowave.datastore.foundationdb.util.FoundationDBClient;
import org.locationtech.geowave.datastore.foundationdb.util.FoundationDBIndexTable;
import org.locationtech.geowave.datastore.foundationdb.util.FoundationDBUtils;

/**
 * This class provides an interface for persisting GeoWave data rows.
 */
public class FoundationDBWriter implements RowWriter {

  private final FoundationDBClient client;
  private final short adapterId;

  // tableCache is where we store the mapping from keys (byte arrays) to GeoWave values
  private final LoadingCache<ByteArray, FoundationDBIndexTable> tableCache =
      Caffeine.newBuilder().build(partitionKey -> getTable(partitionKey.getBytes()));
  private final boolean isTimestampRequired;
  private final String indexNamePrefix;

  /**
   * Construct an instance using a FDB Client.
   * 
   * @param client The client.
   * @param adapterId TODO
   * @param typeName TODO
   * @param indexName TODO
   * @param isTimestampRequired TODO
   */
  public FoundationDBWriter(
      final FoundationDBClient client,
      final short adapterId,
      final String typeName,
      final String indexName,
      final boolean isTimestampRequired) {
    this.client = client;
    this.adapterId = adapterId;
    this.isTimestampRequired = isTimestampRequired;
    this.indexNamePrefix = FoundationDBUtils.getTablePrefix(typeName, indexName);
  }

  /**
   * This method is used to access the relevant table from client's indexCacheTable using a key.
   * 
   * @param partitionKey The key to be used to get the relevant table.
   * @return The table that was mapped to partitionKey.
   */
  private FoundationDBIndexTable getTable(final byte[] partitionKey) {
    return FoundationDBUtils.getIndexTableFromPrefix(
        client,
        indexNamePrefix,
        adapterId,
        partitionKey,
        isTimestampRequired);
  }

  /**
   * Write multiple GeoWave rows to the DB.
   *
   * Preconditions: <ul> <li>The writer is not closed</li> </ul>
   *
   * @param rows The array of rows to be written.
   */
  @Override
  public void write(final GeoWaveRow[] rows) {
    for (final GeoWaveRow row : rows) {
      write(row);
    }
  }

  /**
   * Write a GeoWave row to the DB.
   *
   * <ul> <li> First, we create a partition key based on whether the row contains one. If it does
   * not, we create an empty partition key </li> <li> Next, for each value in the row, the partition
   * key is used to create a mapping from the key (sortKey) to the value in tableCache </li> </ul>
   *
   * Preconditions: <ul> <li>The writer is not closed</li> </ul>
   *
   * @param row The row to be written.
   */
  @Override
  public void write(final GeoWaveRow row) {

    ByteArray partitionKey = null;
    if ((row.getPartitionKey() == null) || (row.getPartitionKey().length == 0)) {
      partitionKey = FoundationDBUtils.EMPTY_PARTITION_KEY;
    } else {
      partitionKey = new ByteArray(row.getPartitionKey());
    }
    for (final GeoWaveValue value : row.getFieldValues()) {
      tableCache.get(partitionKey).add(
          row.getSortKey(),
          row.getDataId(),
          (short) row.getNumberOfDuplicates(),
          value);
    }
  }

  /**
   * Flush the writer, committing all pending writes. Note that the writes may already be committed
   * - this method just establishes that they *must* be committed after the method returns.
   *
   * Preconditions: <ul> <li>The writer is not closed</li> </ul>
   */
  @Override
  public void flush() {
    tableCache.asMap().forEach((k, v) -> v.flush());
  }


  /**
   * Close the writer, preventing any further writes. After calling this method, flush and write may
   * no longer be called. After the writer is closed, there is no way to re-open it.
   */
  @Override
  public void close() {
    flush();
    tableCache.invalidateAll();
  }
}

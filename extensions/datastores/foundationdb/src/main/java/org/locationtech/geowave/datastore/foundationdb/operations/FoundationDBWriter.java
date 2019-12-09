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
 * This class is used to write key, value pairs to the database
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
   * This class is used to write key, value pairs to the database
   * 
   * @param client This is the client associated with this writer
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
   * This method is used to access the relevant table from client's indexCacheTable using a key
   * 
   * @param partitionKey this is the key to be used to get the relevant table
   * @return the table that was mapped to partitionKey
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
   * This method is used to write an array of geowave rows to tableCache
   * 
   * @param rows this is the array of geowave rows to be written to tableCache
   */
  @Override
  public void write(final GeoWaveRow[] rows) {
    for (final GeoWaveRow row : rows) {
      write(row);
    }
  }

  /**
   * <ul> <li> This method is used to write a row to tableCache </li> <li> First, we create a
   * partition key based on whether the row contains one. If it does not, we create an empty
   * partition key </li> <li> Next, for each value in the row, the partition key is used to create a
   * mapping from the key (sortKey) to the value in tableCache </li> </ul>
   * 
   * @param row this is the geowave row to be written to tableCache
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

  @Override
  public void flush() {
    tableCache.asMap().forEach((k, v) -> v.flush());
  }


  /**
   * This method is used to flush the table and then invalidate everything in the table
   */
  @Override
  public void close() {
    flush();
    tableCache.invalidateAll();
  }
}

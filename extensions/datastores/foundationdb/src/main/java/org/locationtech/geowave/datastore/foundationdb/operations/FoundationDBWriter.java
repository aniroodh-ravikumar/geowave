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

public class FoundationDBWriter implements RowWriter {
  private final FoundationDBClient client;
  private final short adapterId;
  private final LoadingCache<ByteArray, FoundationDBIndexTable> tableCache =
      Caffeine.newBuilder().build(partitionKey -> getTable(partitionKey.getBytes()));
  private final boolean isTimestampRequired;
  private final String indexNamePrefix;

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

  private FoundationDBIndexTable getTable(final byte[] partitionKey) {
    return FoundationDBUtils.getIndexTableFromPrefix(
        client,
        indexNamePrefix,
        adapterId,
        partitionKey,
        isTimestampRequired);
  }

  @Override
  public void write(final GeoWaveRow[] rows) {
    for (final GeoWaveRow row : rows) {
      write(row);
    }
  }

  @Override
  public void write(final GeoWaveRow row) {

    ByteArray partitionKey = null;
    if ((row.getPartitionKey() == null) || (row.getPartitionKey().length == 0)) {
      partitionKey = FoundationDBUtils.EMPTY_PARTITION_KEY;
    } else {
      partitionKey = new ByteArray(row.getPartitionKey());
    }
    for (final GeoWaveValue value : row.getFieldValues()) {

      // System.out.println(tableCache.asMap().keySet().toString());

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

  @Override
  public void close() {
    flush();
    tableCache.invalidateAll();
  }
}

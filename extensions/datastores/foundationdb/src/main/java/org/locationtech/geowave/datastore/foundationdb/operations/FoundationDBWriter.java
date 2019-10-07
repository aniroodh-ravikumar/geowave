package org.locationtech.geowave.datastore.foundationdb.operations;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.tuple.Tuple;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.core.store.operations.RowWriter;
import org.locationtech.geowave.datastore.foundationdb.util.FoundationDBClient;
import org.locationtech.geowave.datastore.foundationdb.util.FoundationDBIndexTable;
import org.locationtech.geowave.datastore.foundationdb.util.FoundationDBUtils;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import javax.xml.crypto.Data;
import java.time.Instant;

public class FoundationDBWriter implements RowWriter {
  private final FoundationDBClient client;
  // private final String indexNamePrefix;

  private final short adapterId;
  private final LoadingCache<ByteArray, FoundationDBIndexTable> tableCache =
      Caffeine.newBuilder().build(partitionKey -> getTable(partitionKey.getBytes()));
  private final boolean isTimestampRequired;
  private Database db;

  public FoundationDBWriter(
      final FoundationDBClient client,
      final short adapterId,
      final String typeName,
      final String indexName,
      final boolean isTimestampRequired) {
    this.client = client;
    this.adapterId = adapterId;
    // indexNamePrefix = RocksDBUtils.getTablePrefix(typeName, indexName);
    this.isTimestampRequired = isTimestampRequired;
    FDB fdb = FDB.selectAPIVersion(620);
    this.db = fdb.open();
  }

  private FoundationDBIndexTable getTable(final byte[] partitionKey) {
    return null;
    // return RocksDBUtils.getIndexTableFromPrefix(
    // client,
    // indexNamePrefix,
    // adapterId,
    // partitionKey,
    // isTimestampRequired);
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
    final ByteArray partKey = partitionKey;
    for (final GeoWaveValue value : row.getFieldValues()) {
      // tableCache.get(partitionKey).add(
      // row.getSortKey(),
      // row.getDataId(),
      // (short) row.getNumberOfDuplicates(),
      // value);
      Tuple tuple = Tuple.fromBytes(value.getValue());

      // Run an operation on the database
      this.db.run(tr -> {
        tr.set(Tuple.from(partKey).pack(), Tuple.from(tuple).pack());
        return null;
      });
    }
  }

  @Override
  public void flush() {
    tableCache.asMap().forEach((k, v) -> v.flush());
  }

  @Override
  public void close() {
    flush();
    // tableCache.invalidateAll();
    this.db.close();
  }
}

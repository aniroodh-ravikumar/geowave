package org.locationtech.geowave.datastore.foundationdb.util;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncIterator;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.Longs;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FoundationDBIndexTable extends AbstractFoundationDBTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(FoundationDBIndexTable.class);
  private long prevTime = Long.MAX_VALUE;
  private final boolean requiresTimestamp;
  private final byte[] partition;

  public FoundationDBIndexTable(
      final short adapterId,
      final byte[] partition,
      final boolean requiresTimestamp,
      final boolean visibilityEnabled,
      final boolean compactOnWrite,
      final int batchSize,
      final FoundationDBClient client) {
    super(adapterId, visibilityEnabled, compactOnWrite, batchSize, client);
    this.partition = partition;
    this.requiresTimestamp = requiresTimestamp;
  }

  public void delete(final byte[] sortKey, final byte[] dataId) {
    final Database db = getDb();
    try {
      final byte[] prefix = Bytes.concat(sortKey, dataId);
      final byte[] nextPrefix = ByteArrayUtils.getNextPrefix(prefix);
      db.run(tr -> {
        tr.clear(prefix, nextPrefix);
        return null;
      });
    } catch (final Exception e) {
      LOGGER.warn("Unable to delete by sort key and data ID", e);
    }
  }

  public synchronized void add(
      final byte[] sortKey,
      final byte[] dataId,
      final short numDuplicates,
      final GeoWaveValue value) {
    byte[] key;
    byte[] endBytes;
    if (visibilityEnabled) {
      endBytes =
          Bytes.concat(
              value.getVisibility(),
              ByteArrayUtils.shortToByteArray(numDuplicates),
              new byte[] {
                  (byte) value.getVisibility().length,
                  (byte) sortKey.length,
                  (byte) value.getFieldMask().length});
    } else {
      endBytes =
          Bytes.concat(
              ByteArrayUtils.shortToByteArray(numDuplicates),
              new byte[] {(byte) sortKey.length, (byte) value.getFieldMask().length});
    }
    if (requiresTimestamp) {
      // sometimes rows can be written so quickly that they are the exact
      // same millisecond - while Java does offer nanosecond precision,
      // support is OS-dependent. Instead this check is done to ensure
      // subsequent millis are written at least within this ingest
      // process.
      long time = Long.MAX_VALUE - System.currentTimeMillis();
      if (time >= prevTime) {
        time = prevTime - 1;
      }
      prevTime = time;
      key = Bytes.concat(sortKey, dataId, Longs.toByteArray(time), value.getFieldMask(), endBytes);
    } else {
      key = Bytes.concat(sortKey, dataId, value.getFieldMask(), endBytes);
    }
    put(key, value.getValue());
  }

  public CloseableIterator<GeoWaveRow> iterator() {
    Database db = getDb();
    if (db == null) {
      return new CloseableIterator.Empty<>();
    }
    // byte[] start = new byte[0];
    // byte[] end =
    //     new byte[] {
    //         Byte.MAX_VALUE,
    //         Byte.MAX_VALUE,
    //         Byte.MAX_VALUE,
    //         Byte.MAX_VALUE,
    //         Byte.MAX_VALUE,
    //         Byte.MAX_VALUE,
    //         Byte.MAX_VALUE};
    final byte[] start = Tuple.from("").pack();
    final byte[] end = Tuple.from("0xff").pack();
    return iterator(new ByteArrayRange(start, end));
  }

  public CloseableIterator<GeoWaveRow> iterator(final ByteArrayRange range) {
    Database db = getDb();
    if (db == null) {
      return new CloseableIterator.Empty<>();
    }
    AsyncIterator<KeyValue> iterator = db.run(tr -> {
      AsyncIterable<KeyValue> iterable = tr.getRange(range.getStart(), range.getEnd());
      return iterable.iterator();
    });
    return new FoundationDBRowIterator(
            iterator,
        adapterId,
        partition,
        requiresTimestamp,
        visibilityEnabled);
  }

}

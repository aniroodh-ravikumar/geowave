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
      final int batchSize,
      final Database db) {
    super(adapterId, visibilityEnabled, batchSize, db);
    this.partition = partition;
    this.requiresTimestamp = requiresTimestamp;
  }

  /**
   * Deletes the entries in the database associated with
   * the given sortKey and dataId.
   * @param sortKey - The sortKey for the entries to be deleted.
   * @param dataId - The dataId for the entries to be deleted.
   */
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

  /**
   * Adds an entry to the database given the parameters associated with it. The
   * key of the entry contains the sortKey, dataId, time (if the timeRequired
   * field is true), the fieldMask field of the value and endBytes.
   * @param sortKey - The sortKey to be associated with the key of the entry
   * to be created.
   * @param dataId - The dataId to be associated with the key of the entry to
   * be created.
   * @param numDuplicates - The number of duplicates of the entry to be created.
   * @param value - The value to be associated with the entry to be created.
   */
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

  /**
   * Creates an iterator of GeoWaveRow objects throughout the entire range of
   * the database. The start index is the empty string, and the end index is
   * "0xff".
   * @return - The CloseableIterator object created. The iterator is empty if
   * the db object obtained using the getDb method is null.
   */
  public CloseableIterator<GeoWaveRow> iterator() {
    Database db = getDb();
    if (db == null) {
      return new CloseableIterator.Empty<>();
    }
    final byte[] start = Tuple.from("").pack();
    final byte[] end = Tuple.from("0xff").pack();
    return iterator(new ByteArrayRange(start, end));
  }

  /**
   * Creates an iterator of GeoWaveRow objects over the given range.
   * @param range - A ByteArrayRange object that stores the information
   * about the start and end index of the iterator created.
   * @return - The FoundationDBIterator object created. The iterator is empty if
   * the db object obtained using the getDb method is null.
   */
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

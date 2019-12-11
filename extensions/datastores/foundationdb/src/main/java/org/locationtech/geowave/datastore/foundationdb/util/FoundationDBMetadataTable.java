package org.locationtech.geowave.datastore.foundationdb.util;

import com.apple.foundationdb.*;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.Longs;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;

/**
 * This class provides an interface for interacting with FoundationDB, both within the context of
 * GeoWave metadata, and by directly reading and writing FDB keys and values.
 */
public class FoundationDBMetadataTable implements Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(FoundationDBMetadataTable.class);
  private final Database db;
  private final boolean requiresTimestamp;
  private final boolean visibilityEnabled;
  private long prevTime = Long.MAX_VALUE;

  /**
   * Create a FDB metadata table using a FDB instance.
   *
   * Preconditions: <ul> <li>The FDB instance is not closed</li> <li>The FDB instance is not
   * null</li> </ul>
   *
   * @param db The FDB instance.
   * @param requiresTimestamp TODO
   * @param visibilityEnabled TODO
   */
  public FoundationDBMetadataTable(
      Database db,
      final boolean requiresTimestamp,
      final boolean visibilityEnabled) {
    super();
    this.db = db;
    this.requiresTimestamp = requiresTimestamp;
    this.visibilityEnabled = visibilityEnabled;
  }

  /**
   * Get an iterator to read the entire table. Values are loaded lazily as they are needed.
   *
   * * Preconditions: * <ul> * <li>The table is not closed</li> * </ul>
   *
   * @return The iterator.
   */
  public CloseableIterator<GeoWaveMetadata> iterator() {
    if (db == null) {
      return new CloseableIterator.Empty<>();
    }
    AsyncIterator<KeyValue> iterator = db.run(tr -> {
      final byte[] start = Tuple.from("").pack(); // begin key inclusive
      final byte[] end = Tuple.from("0xff").pack(); // end key exclusive
      // We want to return all KeyValue from the db
      // Some values might have empty byte array as keys hence the empty array as `start`
      AsyncIterable<KeyValue> iterable = tr.getRange(start, end);
      return iterable.iterator();
    });
    return new FoundationDBMetadataIterator(
        iterator,
        this.requiresTimestamp,
        this.visibilityEnabled);
  }

  public CloseableIterator<GeoWaveMetadata> iterator(byte[] primaryID) {
    return prefixIterator(primaryID);
  }

  public CloseableIterator<GeoWaveMetadata> iterator(byte[] primaryID, byte[] secondaryID) {
    return prefixIterator(Bytes.concat(primaryID, secondaryID));
  }

  /**
   * Get an iterator to read metadata values with a given key prefix. Values are loaded lazily as
   * they are needed.
   *
   * Preconditions: <ul> <li>The table is not closed</li> </ul>
   *
   * @param prefix The metadata prefix.
   * @return The iterator.
   */
  private CloseableIterator<GeoWaveMetadata> prefixIterator(final byte[] prefix) {
    AsyncIterator<KeyValue> iterator = db.run(tr -> {
      AsyncIterable<KeyValue> iterable = tr.getRange(prefix, ByteArrayUtils.getNextPrefix(prefix));
      return iterable.iterator();
    });
    // TODO: can this class be asynchronous?
    return new FoundationDBMetadataIterator(
        iterator,
        this.requiresTimestamp,
        this.visibilityEnabled);
  }

  /**
   * Delete a metadata value by key.
   *
   * Preconditions: <ul> <li>The table is not closed</li> </ul>
   *
   * @param key The key of the metadata value.
   */
  public void remove(final byte[] key) {
    this.db.run(tr -> {
      tr.clear(key);
      return null;
    });
  }

  /**
   * Persist a metadata value in the DB. The key is constructed as a combination of the primary and
   * secondary ids, and the timestamp (if the timestamp flag was set in the constructor).
   *
   * Preconditions: <ul> <li>The table is not closed</li> </ul>
   *
   * TODO: extract the key constructor into a static method
   *
   * @param value The metadata to persist.
   */
  public void add(final GeoWaveMetadata value) {
    LOGGER.warn("in add of fdb metadata table");
    byte[] key;
    final byte[] secondaryId =
        value.getSecondaryId() == null ? new byte[0] : value.getSecondaryId();
    byte[] endBytes;
    if (visibilityEnabled) {
      final byte[] visibility = value.getVisibility() == null ? new byte[0] : value.getVisibility();

      endBytes =
          Bytes.concat(
              visibility,
              new byte[] {(byte) visibility.length, (byte) value.getPrimaryId().length});
    } else {
      endBytes = new byte[] {(byte) value.getPrimaryId().length};
    }
    if (requiresTimestamp) {
      // sometimes rows can be written so quickly that they are the exact
      // same millisecond - while Java does offer nanosecond precision,
      // support is OS-dependent. Instead this check is done to ensure
      // subsequent millis are written at least within this ingest
      // process.
      long time = Long.MAX_VALUE - System.currentTimeMillis();
      if (time >= prevTime) { // this makes the timestamp unique
        time = prevTime - 1;
      }
      prevTime = time;
      key = Bytes.concat(value.getPrimaryId(), secondaryId, Longs.toByteArray(time), endBytes);
    } else {
      key = Bytes.concat(value.getPrimaryId(), secondaryId, endBytes);
    }
    put(key, value.getValue());
  }

  /**
   * Directly persist a key-value pair in the DB.
   *
   * Preconditions: <ul> <li>The table is not closed</li> </ul>
   *
   * @param key The key.
   * @param value The value.
   */
  public void put(final byte[] key, final byte[] value) {
    LOGGER.warn("METADATA TABLE writing to db");
    db.run(tr -> {
      tr.set(key, value);
      return null;
    });
  }

  /**
   * After calling this method, all pending changes are committed. Note that in the current
   * implementation, changes are committed immediately.
   *
   * Preconditions: <ul> <li>The table is not closed</li> </ul>
   */
  public void flush() {}

  /**
   * Close the table, preventing any further changes or reads. Closing is idempotent, may not be
   * undone.
   */
  public void close() {
    db.close();
  }
}

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

public class FoundationDBMetadataTable implements Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(FoundationDBMetadataTable.class);
  private final Database db;
  private final boolean requiresTimestamp;
  private final boolean visibilityEnabled;
  private long prevTime = Long.MAX_VALUE;

  public FoundationDBMetadataTable(
      Database db,
      final boolean requiresTimestamp,
      final boolean visibilityEnabled) {
    super();
    this.db = db;
    this.requiresTimestamp = requiresTimestamp;
    this.visibilityEnabled = visibilityEnabled;
  }

  public CloseableIterator<GeoWaveMetadata> iterator() {
    if (db == null) {
      return new CloseableIterator.Empty<>();
    }
    AsyncIterator<KeyValue> iterator = db.run(tr -> {
      final byte[] start = Tuple.from("").pack();
      final byte[] end = Tuple.from("0xff").pack();
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

  public void remove(final byte[] key) {
    this.db.run(tr -> {
      tr.clear(key);
      return null;
    });
  }

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

  public void put(final byte[] key, final byte[] value) {
    LOGGER.warn("METADATA TABLE writing to db");
    db.run(tr -> {
      tr.set(key, value);
      return null;
    });
  }

  public void flush() {}

  public void close() {
    db.close();
  }
}

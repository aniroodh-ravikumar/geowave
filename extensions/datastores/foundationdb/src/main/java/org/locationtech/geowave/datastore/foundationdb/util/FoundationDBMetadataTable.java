package org.locationtech.geowave.datastore.foundationdb.util;

import com.apple.foundationdb.*;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncIterator;
import com.google.common.collect.Iterators;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.Longs;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.locationtech.geowave.datastore.foundationdb.operations.FoundationDBOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.xml.crypto.Data;
import java.io.Closeable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class FoundationDBMetadataTable implements Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(FoundationDBMetadataTable.class);
  private final Database db;
  private final boolean requiresTimestamp;
  private final boolean visibilityEnabled;
  private long prevTime = Long.MAX_VALUE;
  private final List<FDBWrite> writes;

  public FoundationDBMetadataTable(
      Database db,
      final boolean requiresTimestamp,
      final boolean visibilityEnabled) {
    super();
    this.db = db;
    this.requiresTimestamp = requiresTimestamp;
    this.visibilityEnabled = visibilityEnabled;
    this.writes = new LinkedList<>();
  }

  public CloseableIterator<GeoWaveMetadata> iterator() {
    if (db == null) {
      return new CloseableIterator.Empty<>();
    }
    LOGGER.warn("HERE!");
    Long version = this.db.run(tr -> {
        try {
          return tr.getReadVersion().get();
        } catch (Exception e) {
          LOGGER.warn("Got exception from readVersion");
          return null;
        }
    });
    LOGGER.warn("GOT VERSION: " + version);
    AsyncIterator iterator = this.db.run(tr -> {
      byte[] start =  new byte[0];
      byte[] end =
              new byte[] {
                      Byte.MAX_VALUE,
                      Byte.MAX_VALUE,
                      Byte.MAX_VALUE,
                      Byte.MAX_VALUE,
                      Byte.MAX_VALUE,
                      Byte.MAX_VALUE,
                      Byte.MAX_VALUE};
      AsyncIterable iterable = tr.getRange(start, end);
      return iterable.iterator();
    });
    LOGGER.warn("GOT ITERATOR");
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
    Transaction txn = this.db.createTransaction();
    AsyncIterable<KeyValue> iterable = txn.getRange(prefix, ByteArrayUtils.getNextPrefix(prefix));
    // TODO: can this class be asynchronous?
    return new FoundationDBMetadataIterator(
        iterable.iterator(),
        this.requiresTimestamp,
        this.visibilityEnabled);
  }

  public void remove(final byte[] key) {
    try {
      this.db.run(tr -> {
        tr.clear(key);
        return null;
      });
    } catch (final FDBException e) {
      LOGGER.warn("Unable to delete metadata", e);
    }
  }

  public void add(final GeoWaveMetadata value) {
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
    write(key, value.getValue());
  }

  public void write(final byte[] key, final byte[] value) {
    writes.add(new FDBWrite(key, value));
  }

  public void flush() {
    db.run(txn -> {
      this.writes.forEach(write -> write.add(txn));
      return null;
    });
  }

  public void close() {
    db.close();
  }
}

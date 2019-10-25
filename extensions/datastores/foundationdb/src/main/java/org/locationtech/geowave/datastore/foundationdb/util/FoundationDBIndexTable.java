package org.locationtech.geowave.datastore.foundationdb.util;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncIterator;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.Longs;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FoundationDBIndexTable extends AbstractFoundationDBTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(FoundationDBIndexTable.class);
  private final boolean requiresTimestamp;
  private final byte[] partition;

  public FoundationDBIndexTable(
      final short adapterId,
      final byte[] partition,
      final boolean requiresTimestamp,
      final boolean visibilityEnabled,
      final boolean compactOnWrite,
      final int batchSize) {
    super(adapterId, visibilityEnabled, compactOnWrite, batchSize);
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
      final GeoWaveValue value) {}

  public CloseableIterator<GeoWaveRow> iterator() {
    return null;
  }

  public CloseableIterator<GeoWaveRow> iterator(final ByteArrayRange range) {
    try {
      Database db = getDb();
      if (db == null) {
        return new CloseableIterator.Empty<>();
      }
      AsyncIterable iterable = db.run(tr -> {
        return tr.getRange(range.getStart(), range.getEnd());
      });
      AsyncIterator iterator = iterable.iterator();
      return new FoundationDBRowIterator(
          iterator,
          adapterId,
          partition,
          requiresTimestamp,
          visibilityEnabled);
    } catch (Exception e) {
      return new CloseableIterator.Empty<>();
    }
  }

}

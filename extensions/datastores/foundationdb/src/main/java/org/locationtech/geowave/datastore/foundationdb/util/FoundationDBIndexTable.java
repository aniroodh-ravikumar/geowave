package org.locationtech.geowave.datastore.foundationdb.util;

import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.Database;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;

public class FoundationDBIndexTable extends AbstractFoundationDBTable {
  private final boolean requiresTimestamp;
  private final byte[] partition;

  public FoundationDBIndexTable(
      final short adapterId,
      final byte[] partition,
      final boolean requiresTimestamp,
      final boolean visibilityEnabled) {
    super(adapterId, visibilityEnabled);
    this.partition = partition;
    this.requiresTimestamp = requiresTimestamp;
  }

  public void delete(final byte[] sortKey, final byte[] dataId) {}

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

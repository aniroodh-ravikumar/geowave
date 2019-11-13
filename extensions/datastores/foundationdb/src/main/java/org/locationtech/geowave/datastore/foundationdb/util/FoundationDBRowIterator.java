package org.locationtech.geowave.datastore.foundationdb.util;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.KeyValue;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;

public class FoundationDBRowIterator extends AbstractFoundationDBIterator<GeoWaveRow> {
  private final short adapterId;
  private final byte[] partition;
  private final boolean containsTimestamp;
  private final boolean visibilityEnabled;

  public FoundationDBRowIterator(
          final short adapterId,
          final byte[] partition,
          final boolean containsTimestamp,
          final boolean visiblityEnabled,
          final Database db,
          final byte[] startId,
          final byte[] endId) {
    super(db,startId,endId);
    this.adapterId = adapterId;
    this.partition = partition;
    this.containsTimestamp = containsTimestamp;
    visibilityEnabled = visiblityEnabled;
  }

  @Override
  protected GeoWaveRow readRow(final KeyValue keyValue) {
    final byte[] key = keyValue.getKey();
    final byte[] value = keyValue.getValue();
    return new FoundationDBRow(
        adapterId,
        partition,
        key,
        value,
        containsTimestamp,
        visibilityEnabled);
  }
}

package org.locationtech.geowave.datastore.foundationdb.util;

import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.KeyValue;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;

public class FoundationDBRowIterator extends AbstractFoundationDBIterator<GeoWaveRow> {
  private final short adapterId;
  private final byte[] partition;
  private final boolean containsTimestamp;
  private final boolean visibilityEnabled;

  public FoundationDBRowIterator(
      final AsyncIterator it,
      final short adapterId,
      final byte[] partition,
      final boolean containsTimestamp,
      final boolean visiblityEnabled) {
    super(it);
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

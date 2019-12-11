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
      final AsyncIterator<KeyValue> it,
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

  /**
   * Creates a new FoundationDBRow given a KeyValue object, using the adapterId, partition,
   * containsTimeStamp and visibilityEnabled fields of the RowIterator.
   * 
   * @param keyValue The key-value pair to be added to the new row.
   * @return The new FoundationDBRow created using the fields mentioned above.
   */
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

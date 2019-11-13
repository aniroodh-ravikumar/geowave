package org.locationtech.geowave.datastore.foundationdb.util;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.async.AsyncIterator;
import org.locationtech.geowave.core.store.base.dataidx.DataIndexUtils;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import com.apple.foundationdb.KeyValue;

public class FoundationDBDataIndexRowIterator extends AbstractFoundationDBIterator<GeoWaveRow> {
  private final short adapterId;
  private final boolean visibilityEnabled;

  public FoundationDBDataIndexRowIterator(
      final short adapterId,
      final boolean visiblityEnabled,
      final Database db,
      final byte[] startId,
      final byte[] endId) {
    super(db,startId,endId);
    this.adapterId = adapterId;
    this.visibilityEnabled = visiblityEnabled;
  }

  @Override
  protected GeoWaveRow readRow(final KeyValue keyValue) {
    final byte[] key = keyValue.getKey();
    final byte[] value = keyValue.getValue();
    return DataIndexUtils.deserializeDataIndexRow(key, adapterId, value, visibilityEnabled);
  }
}

package org.locationtech.geowave.datastore.foundationdb.util;

import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FoundationDBDataIndexTable extends AbstractFoundationDBTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(FoundationDBDataIndexTable.class);

  public FoundationDBDataIndexTable(
      final String subDirectory,
      final short adapterId,
      final boolean visibilityEnabled,
      final boolean compactOnWrite,
      final int batchSize) {
    super(adapterId, visibilityEnabled, compactOnWrite, batchSize);
  }

  public synchronized void add(final byte[] dataId, final GeoWaveValue value) {}

  public CloseableIterator<GeoWaveRow> dataIndexIterator(final byte[][] dataIds) {
    return null;
  }

  public CloseableIterator<GeoWaveRow> dataIndexIterator(
      final byte[] startDataId,
      final byte[] endDataId) {
    return null;
  }
}

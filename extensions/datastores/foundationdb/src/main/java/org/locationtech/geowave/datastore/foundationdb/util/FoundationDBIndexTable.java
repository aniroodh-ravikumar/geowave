package org.locationtech.geowave.datastore.foundationdb.util;

import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FoundationDBIndexTable extends AbstractFoundationDBTable {

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
    return null;
  }


}

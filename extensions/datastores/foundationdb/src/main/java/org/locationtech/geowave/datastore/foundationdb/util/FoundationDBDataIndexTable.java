package org.locationtech.geowave.datastore.foundationdb.util;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.FDBException;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.base.dataidx.DataIndexUtils;
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
    //
  }

  public synchronized void add(final byte[] dataId, final GeoWaveValue value) {
    // put(dataId, DataIndexUtils.serializeDataIndexValue(value, visibilityEnabled));
  }

  public CloseableIterator<GeoWaveRow> dataIndexIterator(final byte[][] dataIds) {
    return null;
  }

  public CloseableIterator<GeoWaveRow> dataIndexIterator(
      final byte[] startDataId,
      final byte[] endDataId) {
    return null;
  }
}

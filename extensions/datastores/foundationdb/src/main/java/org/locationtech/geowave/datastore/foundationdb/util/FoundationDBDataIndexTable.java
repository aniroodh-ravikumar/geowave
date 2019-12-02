package org.locationtech.geowave.datastore.foundationdb.util;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncIterator;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import com.apple.foundationdb.tuple.Tuple;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.base.dataidx.DataIndexUtils;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FoundationDBDataIndexTable extends AbstractFoundationDBTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(FoundationDBDataIndexTable.class);
  private Database db;

  public FoundationDBDataIndexTable(
      final short adapterId,
      final boolean visibilityEnabled,
      final int batchSize,
      final Database db) {
    super(adapterId, visibilityEnabled, batchSize, db);
  }

  public synchronized void add(final byte[] dataId, final GeoWaveValue value) {
    put(dataId, DataIndexUtils.serializeDataIndexValue(value, visibilityEnabled));
  }

  public CloseableIterator<GeoWaveRow> dataIndexIterator(final byte[][] dataIds) {
    Database db = getDb();
    if (db == null) {
      return new CloseableIterator.Empty<>();
    }
    try {
      final List<byte[]> dataIdsList = Arrays.asList(dataIds);
      final HashMap<byte[], byte[]> dataIdxResults = new HashMap<>();
      for (byte[] dataId : dataIds) {
        byte[] value = db.run(tr -> {
          try {
            return tr.get(dataId).get();
          } catch (final Exception e) {
            LOGGER.error("Failed to get value for dataID", e);
            return null;
          }
        });
        dataIdxResults.put(dataId, value);
      }
      return new CloseableIterator.Wrapper(
          dataIdsList.stream().filter(dataId -> dataIdxResults.containsKey(dataId)).map(
              dataId -> DataIndexUtils.deserializeDataIndexRow(
                  dataId,
                  adapterId,
                  dataIdxResults.get(dataId),
                  visibilityEnabled)).iterator());
    } catch (final Exception e) {
      LOGGER.error("Unable to get values by data ID", e);
    }
    return null;
  }

  public CloseableIterator<GeoWaveRow> dataIndexIterator(
      final byte[] startDataId,
      final byte[] endDataId) {
    Database db = getDb();
    if (db == null) {
      return new CloseableIterator.Empty<>();
    }

    // final byte[] start = startDataId != null ? startDataId : new byte[0];
    // final byte[] end = endDataId != null ? endDataId
    // : new byte[] {
    // Byte.MAX_VALUE,
    // Byte.MAX_VALUE,
    // Byte.MAX_VALUE,
    // Byte.MAX_VALUE,
    // Byte.MAX_VALUE,
    // Byte.MAX_VALUE,
    // Byte.MAX_VALUE};

    LOGGER.warn("WAITING FOR ITERATOR");
    AsyncIterator<KeyValue> iterator = db.run(tr -> {
      final byte[] start = Tuple.from("").pack();
      final byte[] end = Tuple.from("0xff").pack();
      AsyncIterable<KeyValue> iterable = tr.getRange(start, end);
      return iterable.iterator();
    });
    LOGGER.warn("FINISHED GETTING ITERATOR");
    return new FoundationDBDataIndexRowIterator(iterator, adapterId, visibilityEnabled);
  }
}

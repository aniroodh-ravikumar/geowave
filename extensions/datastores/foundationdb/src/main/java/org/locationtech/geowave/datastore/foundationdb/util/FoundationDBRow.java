package org.locationtech.geowave.datastore.foundationdb.util;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.ArrayUtils;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveValueImpl;
import org.locationtech.geowave.core.store.entities.MergeableGeoWaveRow;
import com.google.common.collect.Lists;

public class FoundationDBRow extends MergeableGeoWaveRow implements GeoWaveRow {
  private List<byte[]> mergedKeys;
  private final byte[] key;
  private final short adapterId;
  private final byte[] partition;
  private final byte[] sortKey;
  private final byte[] dataId;
  private final short duplicates;

  /**
   * Represents a row of entries in a FoundationDB database.
   * 
   * @param adapterId The adapterID corresponding to the row.
   * @param partition The index of the partition in this row.
   * @param key The key in the FDB corresponding to this row.
   * @param value The value stored in the FDB for this row.
   * @param containsTimestamp A Boolean that represents whether or not this row contains a
   *        timestamp. If so, otherBytes is increased by 8 to store this information.
   * @param visibilityEnabled A Boolean that represents whether or not visibility is enabled for
   *        this row, which is filtered accordingly.
   */
  public FoundationDBRow(
      final short adapterId,
      final byte[] partition,
      final byte[] key,
      final byte[] value,
      final boolean containsTimestamp,
      final boolean visibilityEnabled) {
    super();
    int otherBytes = 4;
    this.adapterId = adapterId;
    this.partition = partition;
    this.key = key;
    final ByteBuffer buf = ByteBuffer.wrap(key);
    sortKey = new byte[key[key.length - 2]];
    buf.get(sortKey);
    final byte[] fieldMask = new byte[key[key.length - 1]];
    final byte[] visibility;
    if (visibilityEnabled) {
      visibility = new byte[key[key.length - 3]];
      otherBytes++;
    } else {
      visibility = new byte[0];
    }
    if (containsTimestamp) {
      otherBytes += 8;
    }
    dataId =
        new byte[key.length - otherBytes - sortKey.length - fieldMask.length - visibility.length];
    buf.get(dataId);
    if (containsTimestamp) {
      // just skip 8 bytes - we don't care to parse out the timestamp but
      // its there for key uniqueness and to maintain expected sort order
      buf.position(buf.position() + 8);
    }
    buf.get(fieldMask);
    if (visibilityEnabled) {
      buf.get(visibility);
    }
    final byte[] duplicatesBytes = new byte[2];
    buf.get(duplicatesBytes);
    duplicates = ByteArrayUtils.byteArrayToShort(duplicatesBytes);
    attributeValues = Lists.newArrayList(new GeoWaveValueImpl(fieldMask, visibility, value));
  }

  @Override
  public byte[] getDataId() {
    return dataId;
  }

  @Override
  public short getAdapterId() {
    return adapterId;
  }

  @Override
  public byte[] getSortKey() {
    return sortKey;
  }

  @Override
  public byte[] getPartitionKey() {
    return partition;
  }

  @Override
  public int getNumberOfDuplicates() {
    return duplicates;
  }

  public byte[][] getKeys() {
    // this is intentionally not threadsafe because it isn't required
    if (mergedKeys == null) {
      return new byte[][] {key};
    } else {
      return ArrayUtils.add(mergedKeys.toArray(new byte[0][]), key);
    }
  }

  /**
   * Merges an additional FoundationDBRow into the mergedKeys field of the FoundationDBRow object,
   * if the row param is an instance of FoundationDBRow
   * 
   * @param row The row to be merged. Needs to be an instance of FoundationDBRow if it is to be
   *        merged.
   */
  @Override
  public void mergeRow(final MergeableGeoWaveRow row) {
    super.mergeRow(row);
    if (row instanceof FoundationDBRow) {
      // this is intentionally not threadsafe because it isn't required
      if (mergedKeys == null) {
        mergedKeys = new ArrayList<>();
      }
      Arrays.stream(((FoundationDBRow) row).getKeys()).forEach(r -> mergedKeys.add(r));
    }
  }
}

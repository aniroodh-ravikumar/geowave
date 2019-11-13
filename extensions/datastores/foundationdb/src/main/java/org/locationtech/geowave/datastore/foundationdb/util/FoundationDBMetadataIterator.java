package org.locationtech.geowave.datastore.foundationdb.util;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.KeyValue;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncIterator;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;

public class FoundationDBMetadataIterator extends AbstractFoundationDBIterator<GeoWaveMetadata> {

  private final boolean containsTimestamp;
  private final boolean visibilityEnabled;

  public FoundationDBMetadataIterator(
      final boolean containsTimestamp,
      final boolean visibilityEnabled,
      final Database db,
      final byte[] start,
      final byte[] end) {
    super(db,start,end);
    this.containsTimestamp = containsTimestamp;
    this.visibilityEnabled = visibilityEnabled;
    AsyncIterable<KeyValue> iterable = db.run(tr -> tr.getRange(start, end));

  }

  @Override
  protected GeoWaveMetadata readRow(final KeyValue keyValue) {
    final byte[] key = keyValue.getKey();
    final byte[] value = keyValue.getValue();
    final ByteBuffer buf = ByteBuffer.wrap(key);
    final byte[] primaryId = new byte[Byte.toUnsignedInt(key[key.length - 1])];
    final byte[] visibility;

    if (visibilityEnabled) {
      visibility = new byte[Byte.toUnsignedInt(key[key.length - 2])];
    } else {
      visibility = new byte[0];
    }
    int secondaryIdLength = Math.max(key.length - primaryId.length - visibility.length - 1,0);
    if (containsTimestamp) {
      secondaryIdLength = Math.max(secondaryIdLength - 8,0);
    }
    if (visibilityEnabled) {
      secondaryIdLength = Math.max(secondaryIdLength - 1,0);;
    }
    final byte[] secondaryId = new byte[secondaryIdLength];
    buf.get(primaryId);
    buf.get(secondaryId);
    if (containsTimestamp) {
      // just skip 8 bytes - we don't care to parse out the timestamp but
      // its there for key uniqueness and to maintain expected sort order
      try {
        buf.position(buf.position() + 8);
      }
      catch (IllegalArgumentException e) {
      }
    }
    if (visibilityEnabled) {
      buf.get(visibility);
    }

    return new FoundationDBGeoWaveMetadata(primaryId, secondaryId, visibility, value, key);
  }
}

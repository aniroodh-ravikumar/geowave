package org.locationtech.geowave.datastore.foundationdb.operations;

import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.locationtech.geowave.core.store.operations.MetadataDeleter;
import org.locationtech.geowave.core.store.operations.MetadataQuery;
import org.locationtech.geowave.core.store.operations.MetadataType;
import org.locationtech.geowave.datastore.foundationdb.util.FoundationDBGeoWaveMetadata;
import org.locationtech.geowave.datastore.foundationdb.util.FoundationDBMetadataTable;

/**
 * This class provides an abstraction for deleting metadata.
 */
public class FoundationDBMetadataDeleter implements MetadataDeleter {

  private final FoundationDBMetadataTable table;
  private final MetadataType metadataType;
  private boolean closed = false;

  /**
   * Create a deleter for a given metadata type using a FDB Metadata table.
   *
   * Preconditions: <ul> <li>The table is not closed</li> </ul>
   *
   * @param table The table.
   * @param metadataType The type of the metadata to read.
   */
  public FoundationDBMetadataDeleter(
      final FoundationDBMetadataTable table,
      final MetadataType metadataType) {
    this.table = table;
    this.metadataType = metadataType;
  }

  /**
   * Delete metadata from the DB.
   *
   * Preconditions: <ul> <li>The deleter is not closed</li> </ul>
   *
   * @param query The query that specifies the metadata to be deleted.
   */
  @Override
  public boolean delete(final MetadataQuery query) {
    boolean atLeastOneDeletion = false;

    try (CloseableIterator<GeoWaveMetadata> it =
        new FoundationDBMetadataReader(table, metadataType).query(query, false)) {
      while (it.hasNext()) {
        table.remove(((FoundationDBGeoWaveMetadata) it.next()).getKey());
        atLeastOneDeletion = true;
      }
    }
    return atLeastOneDeletion;
  }

  /**
   * Flush the deleter, committing all pending changes. Note that the changes may already be
   * committed - this method just establishes that they *must* be committed after the method
   * returns.
   *
   * Preconditions: <ul> <li>The deleter is not closed</li> </ul>
   */
  @Override
  public void flush() {
    table.flush();
  }

  /**
   * Close the deleter, preventing any further changes. After calling this method, flush and delete
   * may no longer be called. After the deleter is closed, there is no way to re-open it.
   */
  @Override
  public void close() throws Exception {
    // guard against repeated calls to close
    if (!closed) {
      flush();
      closed = true;
    }
  }
}

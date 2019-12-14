package org.locationtech.geowave.datastore.foundationdb.operations;

import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.locationtech.geowave.core.store.operations.MetadataWriter;
import org.locationtech.geowave.datastore.foundationdb.util.FoundationDBMetadataTable;

/**
 * This class provides an interface for persisting metadata.
 */
public class FoundationDBMetadataWriter implements MetadataWriter {
  private final FoundationDBMetadataTable table;
  private boolean closed = false;

  /**
   * Create a writer using a FDB Metadata table.
   *
   * <ul> <li>The table is not closed</li> </ul>
   *
   * @param table The table.
   */
  public FoundationDBMetadataWriter(FoundationDBMetadataTable table) {
    this.table = table;
  }

  /**
   * Write metadata to the table.
   *
   * Preconditions: <ul> <li>The writer is not closed</li> </ul>
   *
   * @param metadata The metadata.
   */
  @Override
  public void write(GeoWaveMetadata metadata) {
    if (table != null) {
      table.add(metadata);
    }
  }

  /**
   * Flush the writer, committing all pending writes. Note that the writes may already be committed
   * - this method just establishes that they *must* be committed after the method returns.
   *
   * Preconditions: <ul> <li>The writer is not closed</li> </ul>
   */
  @Override
  public void flush() {
    if (table != null) {
      table.flush();
    }
  }

  /**
   * Close the writer, preventing any further writes. After calling this method, flush and write may
   * no longer be called. After the writer is closed, there is no way to re-open it.
   */
  @Override
  public void close() {
    if (!closed) {
      flush();
      closed = true;
    }
  }
}

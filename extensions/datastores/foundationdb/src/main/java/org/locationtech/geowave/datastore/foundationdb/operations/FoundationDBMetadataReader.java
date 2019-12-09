package org.locationtech.geowave.datastore.foundationdb.operations;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.CloseableIteratorWrapper;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.locationtech.geowave.core.store.operations.MetadataQuery;
import org.locationtech.geowave.core.store.operations.MetadataReader;
import org.locationtech.geowave.core.store.operations.MetadataType;
import org.locationtech.geowave.core.store.util.DataStoreUtils;
import org.locationtech.geowave.core.store.util.StatisticsRowIterator;
import org.locationtech.geowave.datastore.foundationdb.util.FoundationDBMetadataTable;
import java.util.Arrays;
import java.util.Iterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides an abstraction for reading metadata.
 */
public class FoundationDBMetadataReader implements MetadataReader {
  private static final Logger LOGGER = LoggerFactory.getLogger(FoundationDBMetadataReader.class);
  private final FoundationDBMetadataTable table;
  private final MetadataType metadataType;

  /**
   * Create a reader for a given metadata type using a FDB Metadata table.
   *
   * Preconditions: - The table is not closed
   *
   * @param table The table.
   * @param metadataType The type of the metadata to read.
   */
  public FoundationDBMetadataReader(
      final FoundationDBMetadataTable table,
      final MetadataType metadataType) {
    this.table = table;
    this.metadataType = metadataType;
  }

  /**
   * Read metadata, as specified by the query.
   *
   * @param query The query.
   * @param mergeStats TODO what does this do?
   * @return An iterator that lazily loads the metadata as they are requested.
   */
  public CloseableIterator<GeoWaveMetadata> query(
      final MetadataQuery query,
      final boolean mergeStats) {
    CloseableIterator<GeoWaveMetadata> originalResults;
    Iterator<GeoWaveMetadata> resultsIt;

    if (query.hasPrimaryId()) {
      originalResults = table.iterator(query.getPrimaryId());
      resultsIt = originalResults;
    } else {
      originalResults = table.iterator();
      resultsIt = originalResults;
    }
    if (query.hasPrimaryId() || query.hasSecondaryId()) {
      resultsIt = Iterators.filter(resultsIt, new Predicate<GeoWaveMetadata>() {

        @Override
        public boolean apply(final GeoWaveMetadata input) {
          if (query.hasPrimaryId()
              && !DataStoreUtils.startsWithIfStats(
                  input.getPrimaryId(),
                  query.getPrimaryId(),
                  metadataType)) {
            return false;
          }
          if (query.hasSecondaryId()
              && !Arrays.equals(input.getSecondaryId(), query.getSecondaryId())) {
            return false;
          }
          return true;
        }
      });
    }
    final boolean isStats = MetadataType.STATS.equals(metadataType) && mergeStats;
    final CloseableIterator<GeoWaveMetadata> retVal =
        new CloseableIteratorWrapper<>(originalResults, resultsIt);
    return isStats ? new StatisticsRowIterator(retVal, query.getAuthorizations()) : retVal;
  }

  /**
   * Read metadata, as specified by the query.
   *
   * @param query The query.
   * @return An iterator that lazily loads the metadata as they are requested.
   */
  @Override
  public CloseableIterator<GeoWaveMetadata> query(MetadataQuery query) {
    return this.query(query, true);
  }
}

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

public class FoundationDBMetadataReader implements MetadataReader {
  private static final Logger LOGGER = LoggerFactory.getLogger(FoundationDBMetadataReader.class);
  private final FoundationDBMetadataTable table;
  private final MetadataType metadataType;

  public FoundationDBMetadataReader(
      final FoundationDBMetadataTable table,
      final MetadataType metadataType) {
    this.table = table;
    this.metadataType = metadataType;
  }

  public CloseableIterator<GeoWaveMetadata> query(
      final MetadataQuery query,
      final boolean mergeStats) {
    CloseableIterator<GeoWaveMetadata> originalResults;
    Iterator<GeoWaveMetadata> resultsIt;
    if (query.hasPrimaryId()) {
      originalResults = table.iterator(query.getPrimaryId());
      resultsIt = originalResults;
    } else if (query.hasPrimaryId() && query.hasSecondaryId()) {
      originalResults = table.iterator(query.getPrimaryId(), query.getSecondaryId());
      resultsIt = originalResults;
    } else {
      // TODO figure out the length of a typical primaryID array
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

  @Override
  public CloseableIterator<GeoWaveMetadata> query(MetadataQuery query) {
    return this.query(query, true);
  }
}

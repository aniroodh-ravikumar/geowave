package org.locationtech.geowave.datastore.foundationdb.operations;

import java.io.Closeable;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Stream;
import org.locationtech.geowave.core.index.SinglePartitionQueryRanges;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.CloseableIteratorWrapper;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveRowIteratorTransformer;
import org.locationtech.geowave.core.store.entities.GeoWaveRowMergingIterator;
import org.locationtech.geowave.core.store.operations.DataIndexReaderParams;
import org.locationtech.geowave.core.store.operations.RangeReaderParams;
import org.locationtech.geowave.core.store.operations.ReaderParams;
import org.locationtech.geowave.core.store.operations.RowReader;
import org.locationtech.geowave.core.store.query.filter.ClientVisibilityFilter;
import org.locationtech.geowave.core.store.util.DataStoreUtils;
import org.locationtech.geowave.datastore.foundationdb.util.FoundationDBClient;
import org.locationtech.geowave.mapreduce.splits.RecordReaderParams;
import com.google.common.collect.Streams;

public class FoundationDBReader<T> implements RowReader<T> {
  private final CloseableIterator<T> iterator;

  public FoundationDBReader(
      final FoundationDBClient client,
      final ReaderParams<T> readerParams,
      final boolean async) {
    this.iterator =
        createIteratorForReader(client, readerParams, readerParams.getRowTransformer(), false);
  }

  public FoundationDBReader(
      final FoundationDBClient client,
      final RecordReaderParams recordReaderParams) {
    this.iterator = createIteratorForRecordReader(client, recordReaderParams);
  }

  public FoundationDBReader(
      final FoundationDBClient client,
      final DataIndexReaderParams dataIndexReaderParams) {
    this.iterator = new Wrapper(createIteratorForDataIndexReader(client, dataIndexReaderParams));
  }

  private CloseableIterator<T> createIteratorForReader(
      final FoundationDBClient client,
      final ReaderParams<T> readerParams,
      final GeoWaveRowIteratorTransformer<T> rowTransformer,
      final boolean async) {
    return null;
  }

  private CloseableIterator<T> createIterator(
      final FoundationDBClient client,
      final RangeReaderParams<T> readerParams,
      final GeoWaveRowIteratorTransformer<T> rowTransformer,
      final Collection<SinglePartitionQueryRanges> ranges,
      final Set<String> authorizations,
      final boolean async) {
    return null;
  }

  private CloseableIterator<T> createIteratorForRecordReader(
      final FoundationDBClient client,
      final RecordReaderParams recordReaderParams) {
    return null;
  }

  private Iterator<GeoWaveRow> createIteratorForDataIndexReader(
      final FoundationDBClient client,
      final DataIndexReaderParams dataIndexReaderParams) {
    return null;
  }

  private CloseableIterator<T> wrapResults(
      final Closeable closeable,
      final Iterator<GeoWaveRow> results,
      final RangeReaderParams<T> params,
      final GeoWaveRowIteratorTransformer<T> rowTransformer,
      final Set<String> authorizations,
      final boolean visibilityEnabled) {
    Stream<GeoWaveRow> stream = Streams.stream(results);
    if (visibilityEnabled) {
      stream = stream.filter(new ClientVisibilityFilter(authorizations));
    }
    final Iterator<GeoWaveRow> iterator = stream.iterator();
    return new CloseableIteratorWrapper<>(
        closeable,
        rowTransformer.apply(
            sortBySortKeyIfRequired(
                params,
                DataStoreUtils.isMergingIteratorRequired(params, visibilityEnabled)
                    ? new GeoWaveRowMergingIterator(iterator)
                    : iterator)));
  }

  private static Iterator<GeoWaveRow> sortBySortKeyIfRequired(
      final RangeReaderParams<?> params,
      final Iterator<GeoWaveRow> it) {
    return null;
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public T next() {
    return iterator.next();
  }

  @Override
  public void close() {
    iterator.close();
  }

}

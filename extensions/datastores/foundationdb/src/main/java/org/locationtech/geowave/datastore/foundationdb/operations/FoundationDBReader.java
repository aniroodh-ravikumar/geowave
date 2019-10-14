package org.locationtech.geowave.datastore.foundationdb.operations;

import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.geowave.core.index.ByteArrayRange;
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
import org.locationtech.geowave.datastore.foundationdb.util.FoundationDBDataIndexTable;
import org.locationtech.geowave.datastore.foundationdb.util.FoundationDBUtils;
import org.locationtech.geowave.mapreduce.splits.GeoWaveRowRange;
import org.locationtech.geowave.mapreduce.splits.RecordReaderParams;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
    final Collection<SinglePartitionQueryRanges> ranges =
            readerParams.getQueryRanges().getPartitionQueryRanges();

    final Set<String> authorizations = Sets.newHashSet(readerParams.getAdditionalAuthorizations());
    if ((ranges != null) && !ranges.isEmpty()) {
      return createIterator(
              client,
              readerParams,
              readerParams.getRowTransformer(),
              ranges,
              authorizations,
              async);
    } else {
      final List<CloseableIterator<GeoWaveRow>> iterators = new ArrayList<>();
      for (final short adapterId : readerParams.getAdapterIds()) {
        final Pair<Boolean, Boolean> groupByRowAndSortByTime =
                FoundationDBUtils.isGroupByRowAndIsSortByTime(readerParams, adapterId);
        final String indexNamePrefix =
                FoundationDBUtils.getTablePrefix(
                        readerParams.getInternalAdapterStore().getTypeName(adapterId),
                        readerParams.getIndex().getName());
        final Stream<CloseableIterator<GeoWaveRow>> streamIt =
                FoundationDBUtils.getPartitions(client.getSubDirectory(), indexNamePrefix).stream().map(
                        p -> FoundationDBUtils.getIndexTableFromPrefix(
                                client,
                                indexNamePrefix,
                                adapterId,
                                p.getBytes(),
                                groupByRowAndSortByTime.getRight()).iterator());
        iterators.addAll(streamIt.collect(Collectors.toList()));
      }
      return wrapResults(new Closeable() {
                           AtomicBoolean closed = new AtomicBoolean(false);

                           @Override
                           public void close() throws IOException {
                             if (!closed.getAndSet(true)) {
                               iterators.forEach(it -> it.close());
                             }
                           }
                         },
              Iterators.concat(iterators.iterator()),
              readerParams,
              rowTransformer,
              authorizations,
              client.isVisibilityEnabled());
    }
  }

  private CloseableIterator<T> createIterator(
      final FoundationDBClient client,
      final RangeReaderParams<T> readerParams,
      final GeoWaveRowIteratorTransformer<T> rowTransformer,
      final Collection<SinglePartitionQueryRanges> ranges,
      final Set<String> authorizations,
      final boolean async) {
    final Iterator<CloseableIterator> it =
            Arrays.stream(ArrayUtils.toObject(readerParams.getAdapterIds())).map(
                    adapterId -> new FoundationDBQueryExecution(
                            client,
                            FoundationDBUtils.getTablePrefix(
                                    readerParams.getInternalAdapterStore().getTypeName(adapterId),
                                    readerParams.getIndex().getName()),
                            adapterId,
                            rowTransformer,
                            ranges,
                            new ClientVisibilityFilter(authorizations),
                            DataStoreUtils.isMergingIteratorRequired(
                                    readerParams,
                                    client.isVisibilityEnabled()),
                            async,
                            FoundationDBUtils.isGroupByRowAndIsSortByTime(readerParams, adapterId),
                            FoundationDBUtils.isSortByKeyRequired(readerParams)).results()).iterator();
    final CloseableIterator<T>[] itArray = Iterators.toArray(it, CloseableIterator.class);
    return new CloseableIteratorWrapper<>(new Closeable() {
      AtomicBoolean closed = new AtomicBoolean(false);

      @Override
      public void close() throws IOException {
        if (!closed.getAndSet(true)) {
          Arrays.stream(itArray).forEach(it -> it.close());
        }
      }
    }, Iterators.concat(itArray));
  }

  private CloseableIterator<T> createIteratorForRecordReader(
      final FoundationDBClient client,
      final RecordReaderParams recordReaderParams) {
    final GeoWaveRowRange range = recordReaderParams.getRowRange();
    final byte[] startKey = range.isInfiniteStartSortKey() ? null : range.getStartSortKey();
    final byte[] stopKey = range.isInfiniteStopSortKey() ? null : range.getEndSortKey();
    final SinglePartitionQueryRanges partitionRange =
            new SinglePartitionQueryRanges(
                    range.getPartitionKey(),
                    Collections.singleton(new ByteArrayRange(startKey, stopKey)));
    final Set<String> authorizations =
            Sets.newHashSet(recordReaderParams.getAdditionalAuthorizations());
    return createIterator(
            client,
            (RangeReaderParams<T>) recordReaderParams,
            (GeoWaveRowIteratorTransformer<T>) GeoWaveRowIteratorTransformer.NO_OP_TRANSFORMER,
            Collections.singleton(partitionRange),
            authorizations,
            // there should already be sufficient parallelism created by
            // input splits for record reader use cases
            false);
  }

  private Iterator<GeoWaveRow> createIteratorForDataIndexReader(
      final FoundationDBClient client,
      final DataIndexReaderParams dataIndexReaderParams) {
    final FoundationDBDataIndexTable dataIndexTable =
            FoundationDBUtils.getDataIndexTable(
                    client,
                    dataIndexReaderParams.getInternalAdapterStore().getTypeName(
                            dataIndexReaderParams.getAdapterId()),
                    dataIndexReaderParams.getAdapterId());
    Iterator<GeoWaveRow> iterator;
    if (dataIndexReaderParams.getDataIds() != null) {
      iterator = dataIndexTable.dataIndexIterator(dataIndexReaderParams.getDataIds());
    } else {
      iterator =
              dataIndexTable.dataIndexIterator(
                      dataIndexReaderParams.getStartInclusiveDataId(),
                      dataIndexReaderParams.getEndInclusiveDataId());
    }
    if (client.isVisibilityEnabled()) {
      Stream<GeoWaveRow> stream = Streams.stream(iterator);
      final Set<String> authorizations =
              Sets.newHashSet(dataIndexReaderParams.getAdditionalAuthorizations());
      stream = stream.filter(new ClientVisibilityFilter(authorizations));
      iterator = stream.iterator();
    }
    return iterator;
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
    if (FoundationDBUtils.isSortByKeyRequired(params)) {
      return FoundationDBUtils.sortBySortKey(it);
    }
    return it;
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

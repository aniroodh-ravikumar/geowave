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
import com.apple.foundationdb.subspace.Subspace;
import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FoundationDBReader<T> implements RowReader<GeoWaveRow> {
  private final CloseableIterator<GeoWaveRow> iterator;

  public FoundationDBReader(
      final FoundationDBClient client,
      final ReaderParams<GeoWaveRow> readerParams,
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
    this.iterator = new Wrapper<>(createIteratorForDataIndexReader(client, dataIndexReaderParams));
  }

  private CloseableIterator<GeoWaveRow> createIteratorForReader(
      final FoundationDBClient client,
      final ReaderParams<GeoWaveRow> readerParams,
      final GeoWaveRowIteratorTransformer<GeoWaveRow> rowTransformer,
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
            FoundationDBUtils.getPartitions(
                client.getSubDirectorySubspace(),
                indexNamePrefix).stream().map(
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

  /**
   * Creates a closeable iterator of GeowaveRow objects.
   * 
   * @param client The foundationDBClient associated with the FoundationDBReader object.
   * @param readerParams A RangeReaderParams object that is used to find the adapterIds associated
   *        with the iterator to be created.
   * @param rowTransformer A GeoWaveRowIteratorTransformer object that allows for GeoWaveRow objects
   *        in the iterator to be transformed into a different datatype.
   * @param ranges The SinglePartitionQueryRanges that the iterator is to have elements over.
   * @param authorizations A string of authorizations that is used to create a
   *        ClientVisibilityFilter associated with the iterator.
   * @param async A boolean that represents whether or not the iterator is meant to be asynchronous.
   * @return A new iterator over the ranges specified for the client associated with the given
   *         reader, given the readerParams, and filtered by the authorizations. The result is
   *         wrapped in a CloseableIteratorWrapper.
   */
  private CloseableIterator<GeoWaveRow> createIterator(
      final FoundationDBClient client,
      final RangeReaderParams<GeoWaveRow> readerParams,
      final GeoWaveRowIteratorTransformer<GeoWaveRow> rowTransformer,
      final Collection<SinglePartitionQueryRanges> ranges,
      final Set<String> authorizations,
      final boolean async) {
    final List<CloseableIterator<GeoWaveRow>> iterators =
        Arrays.stream(ArrayUtils.toObject(readerParams.getAdapterIds())).map(
            adapterId -> new FoundationDBQueryExecution<>(
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
                FoundationDBUtils.isSortByKeyRequired(readerParams)).results()).collect(
                    Collectors.toList());
    return new CloseableIteratorWrapper<>(new Closeable() {
      AtomicBoolean closed = new AtomicBoolean(false);

      @Override
      public void close() {
        if (!closed.getAndSet(true)) {
          iterators.forEach(CloseableIterator::close);
        }
      }
    }, Iterators.concat(iterators.iterator()));
  }

  /**
   * Creates an iterator for a record Reader of GeowaveRow objects. Calls the createIterator method
   * with the required parameters.
   * 
   * @param client The foundationDBClient associated with the FoundationDBReader object.
   * @param recordReaderParams The parameters to be associated with the iterator for the record
   *        reader, that provides the range and authorizations for the iterator.
   * @return A new iterator for the client associated with the given reader given the
   *         recordReaderParams. The iterator is wrapped around a CloseableIteratorWrapper.
   */
  private CloseableIterator<GeoWaveRow> createIteratorForRecordReader(
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
        recordReaderParams,
        GeoWaveRowIteratorTransformer.NO_OP_TRANSFORMER,
        Collections.singleton(partitionRange),
        authorizations,
        // there should already be sufficient parallelism created by
        // input splits for record reader use cases
        false);
  }

  /**
   * Creates an iterator for a data index Reader of GeowaveRow objects.
   * 
   * @param client The foundationDBClient associated with the FoundationDBReader object.
   * @param dataIndexReaderParams The parameters to be associated with the iterator for the data
   *        index reader, that provides the adapterIds, dataIds and additional authorizations for
   *        the iterator.
   * @return A new iterator for the client associated with the given reader given the
   *         dataIndexReaderParams.
   */
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

  /**
   * Wraps the given results in a CloseableIteratorWrapper.
   * 
   * @param closeable The closeable object to be wrapped.
   * @param results An iterator of GeoWaveRow objects that is used to create the iterator to be
   *        wrapped.
   * @param params The parameters to be associated with the iterator to be wrapped.
   * @param rowTransformer A GeoWaveRowIteratorTransformer object that allows for GeoWaveRow objects
   *        in the iterator to be transformed into a different datatype.
   * @param authorizations A string of authorizations that is used to create a
   *        ClientVisibilityFilter associated with the iterator.
   * @param visibilityEnabled A boolean that if true, causes the stream (from which the iterator is
   *        created) to be wrapped in a ClientVisibilityFilter with the authorization parameter.
   * @return A CloseableIteratorWrapper of the results given a Closeable object, RangeReaderParams
   *         and authorizations.
   */
  @SuppressWarnings("unchecked")
  private CloseableIterator<GeoWaveRow> wrapResults(
      final Closeable closeable,
      final Iterator<GeoWaveRow> results,
      final RangeReaderParams<GeoWaveRow> params,
      final GeoWaveRowIteratorTransformer<GeoWaveRow> rowTransformer,
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
                    // TODO: why is this a merging iterator?
                    ? new GeoWaveRowMergingIterator(iterator)
                    : iterator)));
  }

  /**
   * Sorts the elements of an iterator by a sortKey if isSortByKeysRequird is true for the given
   * params. Calls the sortBySortKey method in FoundationDBUtils to do this.
   * 
   * @param params A RangeReaderParams object that is used to determine whether the given iterator
   *        needs to be sorted by sortKeys.
   * @param it The iterator of GeoWaveRow objects to be sorted if necessary.
   * @return The input iterator, sorted if necessary (as specified above).
   */
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
  public GeoWaveRow next() {
    return iterator.next();
  }

  /**
   * Calls the close method associated with the iterator field of the class.
   */
  @Override
  public void close() {
    iterator.close();
  }

}

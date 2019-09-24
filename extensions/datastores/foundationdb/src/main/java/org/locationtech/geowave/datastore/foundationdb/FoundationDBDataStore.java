package org.locationtech.geowave.datastore.foundationdb;

import org.locationtech.geowave.core.store.DataStoreOptions;
import org.locationtech.geowave.core.store.metadata.*;
import org.locationtech.geowave.mapreduce.BaseMapReduceDataStore;
import org.locationtech.geowave.mapreduce.MapReduceDataStoreOperations;

public class FoundationDBDataStore extends BaseMapReduceDataStore {
  // TODO: implement FoundationDBOperations
  public FoundationDBDataStore(
      final MapReduceDataStoreOperations operations,
      final DataStoreOptions options) {
    super(
        new IndexStoreImpl(operations, options),
        new AdapterStoreImpl(operations, options),
        new DataStatisticsStoreImpl(operations, options),
        new AdapterIndexMappingStoreImpl(operations, options),
        operations,
        options,
        new InternalAdapterStoreImpl(operations));
  }
}

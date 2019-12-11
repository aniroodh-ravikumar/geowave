/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.foundationdb;

import org.locationtech.geowave.core.store.DataStoreOptions;
import org.locationtech.geowave.core.store.metadata.*;
import org.locationtech.geowave.datastore.foundationdb.operations.FoundationDBOperations;
import org.locationtech.geowave.mapreduce.BaseMapReduceDataStore;
import org.locationtech.geowave.mapreduce.MapReduceDataStoreOperations;
import java.io.Closeable;
import java.io.IOException;

public class FoundationDBDataStore extends BaseMapReduceDataStore implements Closeable {
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

  @Override
  public void close() throws IOException {
    ((FoundationDBOperations) baseOperations).close();
  }
}

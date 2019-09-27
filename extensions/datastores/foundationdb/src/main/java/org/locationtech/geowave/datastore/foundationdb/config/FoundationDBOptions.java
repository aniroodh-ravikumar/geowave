/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.foundationdb.config;

import org.locationtech.geowave.core.store.BaseDataStoreOptions;
import org.locationtech.geowave.datastore.foundationdb.util.FoundationDBUtils;

public class FoundationDBOptions extends BaseDataStoreOptions {

  @Override
  public boolean isServerSideLibraryEnabled() {
    return false;
  }

  @Override
  protected int defaultMaxRangeDecomposition() {
    return FoundationDBUtils.FOUNDATIONDB_DEFAULT_MAX_RANGE_DECOMPOSITION;
  }

  @Override
  protected int defaultAggregationMaxRangeDecomposition() {
    return FoundationDBUtils.FOUNDATIONDB_DEFAULT_AGGREGATION_MAX_RANGE_DECOMPOSITION;
  }

}

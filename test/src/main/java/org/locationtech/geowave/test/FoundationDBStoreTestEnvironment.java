/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test;

import org.locationtech.geowave.core.store.GenericStoreFactory;
import org.locationtech.geowave.core.store.StoreFactoryOptions;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.datastore.foundationdb.FoundationDBStoreFactoryFamily;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FoundationDBStoreTestEnvironment extends StoreTestEnvironment {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(FoundationDBStoreTestEnvironment.class);
  private static final GenericStoreFactory<DataStore> STORE_FACTORY =
      new FoundationDBStoreFactoryFamily().getDataStoreFactory();
  private static FoundationDBStoreTestEnvironment singletonInstance = null;
  private static final String DEFAULT_HOST = "127.0.0.1";
  private static final int DEFAULT_PORT = 4000;

  private FoundationDBLocal fdbLocal;

  public static synchronized FoundationDBStoreTestEnvironment getInstance() {
    if (singletonInstance == null) {
      singletonInstance = new FoundationDBStoreTestEnvironment();
    }
    return singletonInstance;
  }

  @Override
  public void setup() throws Exception {
    if (fdbLocal == null) {
      fdbLocal = new FoundationDBLocal(DEFAULT_HOST, DEFAULT_PORT);
    }

    // Make sure we clean up any old processes first
    if (fdbLocal.isRunning()) {
      fdbLocal.stop();
    }

    fdbLocal.start();
  }

  @Override
  public void tearDown() throws Exception {
    fdbLocal.stop();
  }

  @Override
  protected GenericStoreFactory<DataStore> getDataStoreFactory() {
    return STORE_FACTORY;
  }

  @Override
  protected GeoWaveStoreType getStoreType() {
    return GeoWaveStoreType.FOUNDATIONDB;
  }

  @Override
  protected void initOptions(final StoreFactoryOptions options) {}

  @Override
  public TestEnvironment[] getDependentEnvironments() {
    return new TestEnvironment[] {};
  }
}

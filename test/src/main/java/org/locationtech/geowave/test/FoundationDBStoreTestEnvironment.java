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
import org.locationtech.geowave.datastore.foundationdb.config.FoundationDBRequiredOptions;
import org.locationtech.geowave.datastore.foundationdb.util.FoundationDBClientCache;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FoundationDBStoreTestEnvironment extends StoreTestEnvironment {

	private static final Logger LOGGER = LoggerFactory.getLogger(FoundationDBStoreTestEnvironment.class);
	private static final GenericStoreFactory<DataStore> STORE_FACTORY = new FoundationDBStoreTestEnvironment()
			.getDataStoreFactory();
	private static FoundationDBStoreTestEnvironment singletonInstance = null;

	public static synchronized FoundationDBStoreTestEnvironment getInstance() {
		if (singletonInstance == null) {
			singletonInstance = new FoundationDBStoreTestEnvironment();
		}
		return singletonInstance;
	}

	@Override
	public void setup() throws Exception {

	}

	@Override
	public void tearDown() throws Exception {
	    // this helps clean up any outstanding native resources
	    FoundationDBClientCache.getInstance().closeAll();
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
	protected void initOptions(final StoreFactoryOptions options) {
	}

	@Override
	public TestEnvironment[] getDependentEnvironments() {
		return new TestEnvironment[] {};
	}
}

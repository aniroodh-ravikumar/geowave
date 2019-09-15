/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.foundationdb.config;

import org.locationtech.geowave.core.store.DataStoreOptions;
import org.locationtech.geowave.core.store.StoreFactoryFamilySpi;
import org.locationtech.geowave.core.store.StoreFactoryOptions;
import org.locationtech.geowave.datastore.foundationdb.FoundationDBStoreFactoryFamily;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;

public class FoundationDBRequiredOptions extends StoreFactoryOptions {
  @Parameter(
      names = "--foundationDBMaster",
      required = true,
      description = "An URL for the FoundationDB master node")

  @ParametersDelegate
  private FoundationDBOptions additionalOptions = new FoundationDBOptions();

  public FoundationDBRequiredOptions() {}

  public FoundationDBRequiredOptions(
      final String gwNamespace,
      final FoundationDBOptions additionalOptions) {
    super(gwNamespace);
    this.additionalOptions = additionalOptions;
  }

  @Override
  public StoreFactoryFamilySpi getStoreFactory() {
    return new FoundationDBStoreFactoryFamily();
  }

  @Override
  public DataStoreOptions getStoreOptions() {
    return additionalOptions;
  }

}

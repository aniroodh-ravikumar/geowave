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
      names = "--dir",
      description = "The directory to read/write to.  Defaults to \"foundationdb\" in the working directory.")
  private String dir = "foundationdb";
  @Parameter(
      names = "--compactOnWrite",
      description = "Whether to compact on every write, if false it will only compact on merge. Defaults to true",
      arity = 1)
  private boolean compactOnWrite = true;
  @Parameter(
      names = "--batchWriteSize",
      description = "The size (in records) for each batched write. Anything <= 1 will use synchronous single record writes without batching. Defaults to 1000.")
  private int batchWriteSize = 1000;

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

  public boolean isCompactOnWrite() {
    return compactOnWrite;
  }

  public void setCompactOnWrite(final boolean compactOnWrite) {
    this.compactOnWrite = compactOnWrite;
  }

  public int getBatchWriteSize() {
    return batchWriteSize;
  }

  public void setBatchWriteSize(final int batchWriteSize) {
    this.batchWriteSize = batchWriteSize;
  }

  public void setDirectory(final String dir) {
    this.dir = dir;
  }

  public String getDirectory() {
    return dir;
  }
}

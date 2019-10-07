/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.foundationdb.util;

import java.util.List;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.store.operations.MetadataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FoundationDBUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(FoundationDBUtils.class);

  public static final ByteArray EMPTY_PARTITION_KEY = new ByteArray();
  public static int FOUNDATIONDB_DEFAULT_MAX_RANGE_DECOMPOSITION = 250;
  public static int FOUNDATIONDB_DEFAULT_AGGREGATION_MAX_RANGE_DECOMPOSITION = 250;

  // public static FoundationDBIndexTable getIndexTableFromPrefix(
  // final FoundationDBClient client,
  // final String namePrefix,
  // final short adapterId,
  // final byte[] partitionKey,
  // final boolean requiresTimestamp) {
  // return getIndexTable(
  // client,
  // getTableName(namePrefix, partitionKey),
  // adapterId,
  // partitionKey,
  // requiresTimestamp);
  // }
  //
  // public static FoundationDBIndexTable getIndexTable(
  // final FoundationDBClient client,
  // final String tableName,
  // final short adapterId,
  // final byte[] partitionKey,
  // final boolean requiresTimestamp) {
  // return client.getIndexTable(tableName, adapterId, partitionKey, requiresTimestamp);
  // }

}

/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import org.slf4j.LoggerFactory;
import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;

public class FoundationDBLocal {

  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(FoundationDBLocal.class);

  private Database db;

  public FoundationDBLocal(final String host, final int port) {
    FileOutputStream fileStream = null;
    OutputStreamWriter fw = null;
    try {
      fileStream = new FileOutputStream("./fdb.cluster");
      fw = new OutputStreamWriter(fileStream, "UTF-8");
      fw.write(String.format("test:test@%s:%d", host, port));
    } catch (IOException e) {
      LOGGER.error("Failed to create fdb.cluster file");
    }
    try {
      if (fw != null) {
        fw.close();
      }
    } catch (IOException e) {
      LOGGER.error("Failed to close writer");
    }
  }

  public void start() {
    if (db == null) {
      FDB fdb = FDB.selectAPIVersion(610);
      db = fdb.open(); // Opens using `fdb.cluster` file which is in current directory
    }
  }

  public boolean isRunning() {
    return db != null;
  }

  public void stop() {
    if (db != null) {
      db.close();
    }
  }

}

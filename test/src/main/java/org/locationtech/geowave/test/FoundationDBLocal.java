/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecuteResultHandler;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.Executor;
import org.apache.commons.io.IOUtils;
import org.codehaus.plexus.archiver.tar.TarGZipUnArchiver;
import org.codehaus.plexus.logging.console.ConsoleLogger;
import org.slf4j.LoggerFactory;
import com.jcraft.jsch.Logger;

public class FoundationDBLocal {
  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(FoundationDBLocal.class);

  // these need to move to config
  private static final String FOUNDATIONDB_URL = "https://www.foundationdb.org/downloads/6.1.12/linux/";
  private static final String FOUNDATIONDB_TAR = "fdb_6.1.12.tar.gz";
  // TODO: Change HOST_PORT because DynamoDB uses 8000
  private static final String HOST_PORT = "8000";

  private static final long EMULATOR_SPINUP_DELAY_MS = 30000L;

  private final File foundationDBLocalDir;
  private ExecuteWatchdog watchdog;

  public FoundationDBLocal(final String localDir) {
    if (TestUtils.isSet(localDir)) {
    	foundationDBLocalDir = new File(localDir);
    } else {
    	foundationDBLocalDir = new File(TestUtils.TEMP_DIR, "foundationdb");
    }

    if (!foundationDBLocalDir.exists() && !foundationDBLocalDir.mkdirs()) {
      LOGGER.warn("unable to create directory " + foundationDBLocalDir.getAbsolutePath());
    }
  }

  public boolean start() {
    if (!isInstalled()) {
      try {
        if (!install()) {
          return false;
        }
      } catch (final IOException e) {
        LOGGER.error(e.getMessage());
        return false;
      }
    }

    try {
      startDynamoLocal();
    } catch (IOException | InterruptedException e) {
      LOGGER.error(e.getMessage());
      return false;
    }

    return true;
  }

  public boolean isRunning() {
    return ((watchdog != null) && watchdog.isWatching());
  }

  public void stop() {
    // first, ask the watchdog nicely:
    watchdog.destroyProcess();
  }

  private boolean isInstalled() {
    final File dynLocalJar = new File(foundationDBLocalDir, "DynamoDBLocal.jar");

    return (dynLocalJar.canRead());
  }

  protected boolean install() throws IOException {
    HttpURLConnection.setFollowRedirects(true);
    final URL url = new URL(FOUNDATIONDB_URL + FOUNDATIONDB_TAR);

    final File downloadFile = new File(foundationDBLocalDir, FOUNDATIONDB_TAR);
    if (!downloadFile.exists()) {
      try (FileOutputStream fos = new FileOutputStream(downloadFile)) {
        IOUtils.copyLarge(url.openStream(), fos);
        fos.flush();
      }
    }

    final TarGZipUnArchiver unarchiver = new TarGZipUnArchiver();
    unarchiver.enableLogging(new ConsoleLogger(Logger.WARN, "FoundationDB Local Unarchive"));
    unarchiver.setSourceFile(downloadFile);
    unarchiver.setDestDirectory(foundationDBLocalDir);
    unarchiver.extract();

    if (!downloadFile.delete()) {
      LOGGER.warn("cannot delete " + downloadFile.getAbsolutePath());
    }

    // Check the install
    if (!isInstalled()) {
      LOGGER.error("FoundationDB Local install failed");
      return false;
    }

    return true;
  }

  /**
   * Using apache commons exec for cmd line execution
   *
   * @param command
   * @return exitCode
   * @throws ExecuteException
   * @throws IOException
   * @throws InterruptedException
   */
  private void startDynamoLocal() throws ExecuteException, IOException, InterruptedException {
    // java -Djava.library.path=./FoundationDBLocal_lib -jar FoundationDBLocal.jar
    // -sharedDb
    final CommandLine cmdLine = new CommandLine("java");

    cmdLine.addArgument("-Djava.library.path=" + foundationDBLocalDir + "/DynamoDBLocal_lib");
    cmdLine.addArgument("-jar");
    cmdLine.addArgument(foundationDBLocalDir + "/DynamoDBLocal.jar");
    cmdLine.addArgument("-sharedDb");
    cmdLine.addArgument("-inMemory");
    cmdLine.addArgument("-port");
    cmdLine.addArgument(HOST_PORT);
    System.setProperty("aws.accessKeyId", "dummy");
    System.setProperty("aws.secretKey", "dummy");

    // Using a result handler makes the emulator run async
    final DefaultExecuteResultHandler resultHandler = new DefaultExecuteResultHandler();

    // watchdog shuts down the emulator, later
    watchdog = new ExecuteWatchdog(ExecuteWatchdog.INFINITE_TIMEOUT);
    final Executor executor = new DefaultExecutor();
    executor.setWatchdog(watchdog);
    executor.execute(cmdLine, resultHandler);

    // we need to wait here for a bit, in case the emulator needs to update
    // itself
    Thread.sleep(EMULATOR_SPINUP_DELAY_MS);
  }
}

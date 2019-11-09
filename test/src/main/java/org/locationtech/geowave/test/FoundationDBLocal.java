/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.jcraft.jsch.Logger;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecuteResultHandler;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.codehaus.plexus.archiver.tar.TarGZipUnArchiver;
import org.codehaus.plexus.logging.console.ConsoleLogger;
import org.slf4j.LoggerFactory;

public class FoundationDBLocal {
  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(FoundationDBLocal.class);
  private static final String FDB_REPO_URL = "https://www.foundationdb.org/downloads/6.2.7/ubuntu/installers/";
  private static final String FDB_CLIENT_DEB_PACKAGE = "foundationdb-clients_6.2.7-1_amd64.deb";
  private static final String FDB_SERVER_DEB_PACKAGE = "foundationdb-server_6.2.7-1_amd64.deb";
  private static final String FDB_MONITOR = "fdbmonitor";
  private static final String FDB_CLI = "fdbcli";
  private static final String FDB_SERVER = "fdbserver";
  private static final String FDB_CLUSTER_FILE = "fdb.cluster";
  private static final String FDB_CONF_FILE = "foundationdb.conf";
  private static final long STARTUP_DELAY_MS = 1500L;

  private final File foundationLocalDir;
  private final File foundationDBDir; // storage for database files
  private final String clusterFilePath;
  private final String host;
  private final int port;
  private final List<ExecuteWatchdog> watchdogs;

  private Database db;

  public FoundationDBLocal(final String host, final int port, final String localDir) {
    LOGGER.warn("In init of FDBLocal");
    this.host = host;
    this.port = port;
    if (TestUtils.isSet(localDir)) {
      foundationLocalDir = new File(localDir);
    } else {
      foundationLocalDir = new File(TestUtils.TEMP_DIR, "fdb");
    }
    if (!foundationLocalDir.exists() && !foundationLocalDir.mkdirs()) {
      LOGGER.error("unable to create directory {}", foundationLocalDir.getAbsolutePath());
    } else if (!foundationLocalDir.isDirectory()) {
      LOGGER.error("{} exists but is not a directory", foundationLocalDir.getAbsolutePath());
    }
    foundationDBDir = new File(foundationLocalDir, "db");

    // Create and write to fdb.cluster file
    clusterFilePath = foundationLocalDir.getPath() + "/" + FDB_CLUSTER_FILE;
    FileOutputStream fileStream = null;
    OutputStreamWriter fw = null;
    try {
      fileStream = new FileOutputStream(clusterFilePath);
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

    this.watchdogs = new ArrayList<>();
  }

  public boolean start() {
    if (!isInstalled()) {
      try {
        if (!install()) {
          return false;
        }
      } catch (IOException | ArchiveException e) {
        LOGGER.error("FoundationDBLocal installation error: {}", e.getMessage());
        return false;
      }
    }

    try {
      startFDBServer();
    } catch (IOException | InterruptedException e) {
      LOGGER.error("FDB server start error: {}", e.getMessage());
      return false;
    }

    if (db == null) {
      FDB fdb = FDB.selectAPIVersion(610);
      db = fdb.open(clusterFilePath); // Opens using `fdb.cluster` file
    }

    return true;
  }

  private void startFDBServer() throws ExecuteException, IOException, InterruptedException {
    LOGGER.warn("isInstalled: " + isInstalled());
    if (!isInstalled()) {
      LOGGER.warn("NOT INSTALLED; EXITING");
      return;
    }
    if (!foundationDBDir.exists() && !foundationDBDir.mkdirs()) {
      LOGGER.error("unable to create directory {}", foundationDBDir.getAbsolutePath());
    } else if (!foundationDBDir.isDirectory()) {
      LOGGER.error("{} exists but is not a directory", foundationDBDir.getAbsolutePath());
    }

    final File fdbMonitorBinary = new File(foundationLocalDir.getAbsolutePath(), FDB_MONITOR);
    final File fdbCliBinary = new File(foundationLocalDir.getAbsolutePath(), FDB_CLI);
    final File fdbServerBinary = new File(foundationLocalDir.getAbsolutePath(), FDB_SERVER);

    final CommandLine startMonitor = new CommandLine(fdbMonitorBinary.getAbsolutePath());
    startMonitor.addArgument("--conffile");
    startMonitor.addArgument(new File(foundationLocalDir, FDB_CONF_FILE).getAbsolutePath());
    executeAsyncAndWatch(startMonitor);

    final CommandLine startServer = new CommandLine(fdbServerBinary.getAbsolutePath());
    startServer.addArgument("-p");
    startServer.addArgument(host + ":" + port);
    startServer.addArgument("-C");
    startServer.addArgument(new File(foundationLocalDir, FDB_CLUSTER_FILE).getAbsolutePath());
    executeAsyncAndWatch(startServer);

    final CommandLine configureDatabase = new CommandLine(fdbCliBinary.getAbsolutePath());
    configureDatabase.addArgument("--exec");
    configureDatabase.addArgument("\"configure new single ssd\"");
    executeAsyncAndWatch(configureDatabase);

    Thread.sleep(STARTUP_DELAY_MS);
  }

  private void executeAsyncAndWatch(final CommandLine command)
          throws ExecuteException, IOException {
//    String commandString = String.join(" ", command.toStrings());
//    LOGGER.warn("executing: " + commandString);
//    Runtime rt = Runtime.getRuntime();
//    Process pr = rt.exec(commandString);
//    try {
//      int exitCode = pr.waitFor();
//      LOGGER.warn("exit code: " + exitCode);
//    } catch (InterruptedException e) {
//      LOGGER.warn("Failed executing " + commandString + " for reason: " + e.toString());
//    }
    LOGGER.info("Running async: {}", command.toString());
    try {
    final ExecuteWatchdog watchdog = new ExecuteWatchdog(ExecuteWatchdog.INFINITE_TIMEOUT);
    final DefaultExecutor executor = new DefaultExecutor();
    executor.setWatchdog(watchdog);
    executor.setWorkingDirectory(foundationLocalDir);
    watchdogs.add(watchdog);
    LOGGER.warn("command: " + command.toString());
      // Using a result handler makes the local instance run async
      executor.execute(command, new DefaultExecuteResultHandler());
    } catch (Exception e) {
      LOGGER.warn("FAILED EXECUTE: " + e.getStackTrace());
    }
  }

  private boolean isInstalled() {
    final File fdbMonitorBinary = new File(foundationLocalDir, FDB_MONITOR);
    final File fdbCliBinary = new File(foundationLocalDir, FDB_CLI);
    final File fdbServerBinary = new File(foundationLocalDir, FDB_SERVER);
    final File fdbConfFile = new File(foundationLocalDir, FDB_CONF_FILE);
    LOGGER.warn("CLI PATH: " + fdbCliBinary.getAbsolutePath());
    final boolean okMonitor = fdbMonitorBinary.exists() && fdbMonitorBinary.canExecute();
    final boolean okCli = fdbCliBinary.exists() && fdbCliBinary.canExecute();
    final boolean okServer = fdbServerBinary.exists() && fdbServerBinary.canExecute();
    final boolean okConfFile = fdbConfFile.exists();
    return okMonitor && okCli && okServer && okConfFile;
  }

  private void installDebianPackage(final String debPackage) throws IOException, ArchiveException {
    LOGGER.info("Installing {}", debPackage);
    LOGGER.warn("Downloading package {}", debPackage);
    final File debPackageFile = new File(foundationLocalDir, debPackage);
    if (!debPackageFile.exists()) {
      HttpURLConnection.setFollowRedirects(true);
      final URL url = new URL(FDB_REPO_URL + debPackage);
      try (FileOutputStream fos = new FileOutputStream(debPackageFile)) {
        IOUtils.copy(url.openStream(), fos);
        fos.flush();
      }
    }

    final File debDataTarGz = new File(foundationLocalDir, "data.tar.gz");
    if (!debDataTarGz.exists()) {
      try (FileInputStream fis = new FileInputStream(debPackageFile);
           ArchiveInputStream debInputStream =
                   new ArchiveStreamFactory().createArchiveInputStream("ar", fis)) {
        ArchiveEntry entry = null;
        while ((entry = debInputStream.getNextEntry()) != null) {
          if (debDataTarGz.getName().equals(entry.getName())) {
            try (FileOutputStream fos = new FileOutputStream(debDataTarGz)) {
              IOUtils.copy(debInputStream, fos);
            }
            break;
          }
        }
      }
    }

    LOGGER.warn("Extracting FDB debian package data contents");
    final TarGZipUnArchiver unarchiver = new TarGZipUnArchiver();
    unarchiver.enableLogging(new ConsoleLogger(Logger.WARN, "FDB Local Unarchive " + debPackage));
    unarchiver.setSourceFile(debDataTarGz);
    unarchiver.setDestDirectory(foundationLocalDir);
    unarchiver.extract();

    for (final File f : new File[] {debPackageFile, debDataTarGz}) {
      if (!f.delete()) {
        LOGGER.warn("cannot delete {}", f.getAbsolutePath());
      }
    }
  }

  private boolean install() throws IOException, ArchiveException {
    LOGGER.info("Installing {}", FDB_SERVER_DEB_PACKAGE);

    LOGGER.warn("Downloading FDB client package");
    installDebianPackage(FDB_CLIENT_DEB_PACKAGE);

    LOGGER.warn("Downloading FDB server package");
    installDebianPackage(FDB_SERVER_DEB_PACKAGE);

    LOGGER.warn("Moving fdbmonitor and fdbserver binaries to {}", foundationLocalDir);
    // Move the fdbmonitor binary, fdbserver binary, fdbcli binary, and fdb conf file into the fdb local directory
    final Path fdbLib =
            Paths.get(foundationLocalDir.getAbsolutePath(), "usr", "lib", "foundationdb");
    final Path fdbBin =
            Paths.get(foundationLocalDir.getAbsolutePath(), "usr", "bin");
    final Path fdbSBin =
            Paths.get(foundationLocalDir.getAbsolutePath(), "usr", "sbin");
    final Path fdbEtc =
            Paths.get(foundationLocalDir.getAbsolutePath(), "etc", "foundationdb");
    final File fdbMonitorBinary = fdbLib.resolve(FDB_MONITOR).toFile();
    final File fdbCliBinary = fdbBin.resolve(FDB_CLI).toFile();
    final File fdbServerBinary = fdbSBin.resolve(FDB_SERVER).toFile();
    final File fdbConfFile = fdbEtc.resolve(FDB_CONF_FILE).toFile();
    fdbMonitorBinary.setExecutable(true);
    fdbCliBinary.setExecutable(true);
    fdbServerBinary.setExecutable(true);
    FileUtils.moveFileToDirectory(fdbMonitorBinary, foundationLocalDir, false);
    FileUtils.moveFileToDirectory(fdbCliBinary, foundationLocalDir, false);
    FileUtils.moveFileToDirectory(fdbServerBinary, foundationLocalDir, false);
    FileUtils.moveFileToDirectory(fdbConfFile, foundationLocalDir, false);

    if (isInstalled()) {
      LOGGER.warn("FoundationDBLocal installation successful");
      return true;
    } else {
      LOGGER.error("FoundationDBLocal installation failed");
      return false;
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

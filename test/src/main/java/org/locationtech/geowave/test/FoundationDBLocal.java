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
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.codehaus.plexus.archiver.tar.TarGZipUnArchiver;
import org.codehaus.plexus.logging.console.ConsoleLogger;
import org.slf4j.LoggerFactory;

public class FoundationDBLocal {
  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(FoundationDBLocal.class);
  // macOS
  private static final String FDB_MACOS_REPO_URL =
      "https://www.foundationdb.org/downloads/6.2.7/macOS/installers/";
  private static final String FDB_MACOS_PACKAGE = "FoundationDB-6.2.7.pkg";

  // Ubuntu
  private static final String FDB_REPO_URL =
      "https://www.foundationdb.org/downloads/6.2.7/ubuntu/installers/";
  private static final String FDB_CLIENT_DEB_PACKAGE = "foundationdb-clients_6.2.7-1_amd64.deb";
  private static final String FDB_SERVER_DEB_PACKAGE = "foundationdb-server_6.2.7-1_amd64.deb";
  private static final String FDB_CLI = "fdbcli";
  private static final String FDB_SERVER = "fdbserver";
  private static final String FDB_C = "libfdb_c";
  private static final String FDB_C_DYLIB = FDB_C + ".dylib";
  private static final String FDB_C_SO = FDB_C + ".so";
  private static final String FDB_C_LIBRARY_PATH = "FDB_LIBRARY_PATH_FDB_C";
  private static final String FDB_CLUSTER_FILE = "fdb.cluster";
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
    final String clusterFileContents = String.format("test:test@%s:%d", host, port);
    writeContentToFile(clusterFileContents, clusterFilePath);

    this.watchdogs = new ArrayList<>();
  }

  private void writeContentToFile(final String content, final String path) {
    FileOutputStream fileStream = null;
    OutputStreamWriter fw = null;
    try {
      fileStream = new FileOutputStream(path);
      fw = new OutputStreamWriter(fileStream, "UTF-8");
      fw.write(content);
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
    } catch (InterruptedException e) {
      LOGGER.error("FDB server start error: {}", e.getMessage());
      return false;
    }

    if (db == null) {
      FDB fdb = FDB.selectAPIVersion(610);
      db = fdb.open(clusterFilePath); // Opens using `fdb.cluster` file
    }

    return true;
  }

  private void startFDBServer() throws InterruptedException {
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

    final File fdbCliBinary = new File(foundationLocalDir.getAbsolutePath(), FDB_CLI);
    final File fdbServerBinary = new File(foundationLocalDir.getAbsolutePath(), FDB_SERVER);

    final CommandLine startServer = new CommandLine(fdbServerBinary.getAbsolutePath());
    startServer.addArgument("-p");
    startServer.addArgument(host + ":" + port);
    startServer.addArgument("-C");
    startServer.addArgument(new File(foundationLocalDir, FDB_CLUSTER_FILE).getAbsolutePath());
    executeCommand(startServer, true);

    final CommandLine configureDatabase = new CommandLine(fdbCliBinary.getAbsolutePath());
    configureDatabase.addArgument("--exec");
    configureDatabase.addArgument("configure new single memory", false);
    executeCommand(configureDatabase, false);

    final CommandLine status = new CommandLine(fdbCliBinary.getAbsolutePath());
    status.addArgument("--exec");
    status.addArgument("\"status\"");
    executeCommand(status, false);

    Thread.sleep(STARTUP_DELAY_MS);

    LOGGER.warn("FINISHED STARTUP!");
  }

  private void executeCommand(final CommandLine command, final boolean shouldExecuteAsync) {
    LOGGER.warn("Running command: {}", String.join(" ", command.toStrings()));
    try {
      final ExecuteWatchdog watchdog = new ExecuteWatchdog(ExecuteWatchdog.INFINITE_TIMEOUT);
      final DefaultExecutor executor = new DefaultExecutor();
      executor.setWatchdog(watchdog);
      executor.setWorkingDirectory(foundationLocalDir);
      watchdogs.add(watchdog);
      if (shouldExecuteAsync) {
        // Using a result handler makes the command run async
        executor.execute(command, new DefaultExecuteResultHandler());
      } else {
        // Only passing in command makes the command run sync
        executor.execute(command);
      }
    } catch (Exception e) {
      LOGGER.warn("FAILED EXECUTE: " + e.toString());
    }
  }

  private boolean isInstalled() {
    final File fdbCDylib = new File(foundationLocalDir, FDB_C_DYLIB);
    final File fdbCSo = new File(foundationLocalDir, FDB_C_SO);
    final File fdbCliBinary = new File(foundationLocalDir, FDB_CLI);
    final File fdbServerBinary = new File(foundationLocalDir, FDB_SERVER);
    final boolean okCSo = fdbCSo.exists() && fdbCSo.canExecute();
    final boolean okCDylib = fdbCDylib.exists() && fdbCDylib.canExecute();
    final boolean okCli = fdbCliBinary.exists() && fdbCliBinary.canExecute();
    final boolean okServer = fdbServerBinary.exists() && fdbServerBinary.canExecute();
    boolean okFdbC = false;
    String osName = System.getProperty("os.name").toLowerCase();
    if (osName.indexOf("mac") >= 0) { // Running macOS
      okFdbC = okCDylib;
    } else if (osName.indexOf("nix") >= 0) { // Running nix
      okFdbC = okCSo;
    }
    return okFdbC && okCli && okServer;
  }

  private void installPackageFromURL(final String repoURL, final String packageName)
      throws IOException {
    LOGGER.info("Installing {}", packageName);
    LOGGER.warn("Downloading package {}", packageName);
    final File packageFile = new File(foundationLocalDir, packageName);
    if (!packageFile.exists()) {
      HttpURLConnection.setFollowRedirects(true);
      final URL url = new URL(repoURL + packageName);
      try (FileOutputStream fos = new FileOutputStream(packageFile)) {
        IOUtils.copy(url.openStream(), fos);
        fos.flush();
      }
    }
  }

  private void extractContentsFromDebPackage(final String debPackage)
      throws IOException, ArchiveException {
    LOGGER.info("Extracting deb package {}", debPackage);
    final File debPackageFile = new File(foundationLocalDir, debPackage);
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
    String osName = System.getProperty("os.name").toLowerCase();
    LOGGER.warn("Running on OS: " + osName);
    LOGGER.warn("Installing");
    if (osName.indexOf("mac") >= 0) { // Running macOS
      LOGGER.warn("INSTALLING MACOS");
      try {
        LOGGER.warn("Downloading FDB client and server package");
        installPackageFromURL(FDB_MACOS_REPO_URL, FDB_MACOS_PACKAGE);
        final Path pkg = Paths.get(foundationLocalDir.getAbsolutePath());
        final File pkgFile = pkg.resolve(FDB_MACOS_PACKAGE).toFile();

        // Use pkgutil to extract files from FDB_MACOS_PACKAGE
        final CommandLine pkgUtilExpandFull = new CommandLine("pkgutil");
        pkgUtilExpandFull.addArgument("--expand-full");
        pkgUtilExpandFull.addArgument(pkgFile.getAbsolutePath());
        pkgUtilExpandFull.addArgument("pkg");
        executeCommand(pkgUtilExpandFull, false);

        LOGGER.warn("Moving fdbserver and fdbcli binaries to {}", foundationLocalDir);
        final Path fdbClientPath =
            Paths.get(
                foundationLocalDir.getAbsolutePath(),
                "pkg",
                "FoundationDB-clients.pkg",
                "Payload");
        final Path fdbServerPath =
            Paths.get(
                foundationLocalDir.getAbsolutePath(),
                "pkg",
                "FoundationDB-server.pkg",
                "Payload");

        final Path fdbClientLib =
            Paths.get(fdbClientPath.toAbsolutePath().toString(), "usr", "local", "lib");
        final Path fdbClientBin =
            Paths.get(fdbClientPath.toAbsolutePath().toString(), "usr", "local", "bin");
        final Path fdbServerLibExec =
            Paths.get(fdbServerPath.toAbsolutePath().toString(), "usr", "local", "libexec");

        final File fdbCDylib = fdbClientLib.resolve(FDB_C_DYLIB).toFile();
        final File fdbCliBinary = fdbClientBin.resolve(FDB_CLI).toFile();
        final File fdbServerBinary = fdbServerLibExec.resolve(FDB_SERVER).toFile();

        fdbCDylib.setExecutable(true);
        fdbCliBinary.setExecutable(true);
        fdbServerBinary.setExecutable(true);

        FileUtils.moveFileToDirectory(fdbCDylib, foundationLocalDir, false);
        FileUtils.moveFileToDirectory(fdbCliBinary, foundationLocalDir, false);
        FileUtils.moveFileToDirectory(fdbServerBinary, foundationLocalDir, false);

        // Set FDB_LIBRARY_PATH_FDB_C environment property to path of FDB_C_DYLIB
        final String fdbCDylibPath = foundationLocalDir.getAbsolutePath() + "/" + FDB_C_DYLIB;
        System.setProperty(FDB_C_LIBRARY_PATH, fdbCDylibPath);

        LOGGER.warn("FINISHED INSTALLING");
      } catch (Exception e) {
        LOGGER.warn("FAILED INSTALLING for reason: " + e.toString());
      }
    }

    if (osName.indexOf("nix") >= 0) { // Running nix
      LOGGER.warn("Downloading FDB client package");
      installPackageFromURL(FDB_REPO_URL, FDB_CLIENT_DEB_PACKAGE);
      extractContentsFromDebPackage(FDB_CLIENT_DEB_PACKAGE);

      LOGGER.warn("Downloading FDB server package");
      installPackageFromURL(FDB_REPO_URL, FDB_SERVER_DEB_PACKAGE);
      extractContentsFromDebPackage(FDB_SERVER_DEB_PACKAGE);

      LOGGER.warn("Moving fdbserver and fdbcli binaries to {}", foundationLocalDir);
      final Path fdbLib = Paths.get(foundationLocalDir.getAbsolutePath(), "usr", "lib");
      final Path fdbBin = Paths.get(foundationLocalDir.getAbsolutePath(), "usr", "bin");
      final Path fdbSBin = Paths.get(foundationLocalDir.getAbsolutePath(), "usr", "sbin");

      final File fdbCSo = fdbLib.resolve(FDB_C_SO).toFile();
      final File fdbCliBinary = fdbBin.resolve(FDB_CLI).toFile();
      final File fdbServerBinary = fdbSBin.resolve(FDB_SERVER).toFile();

      fdbCSo.setExecutable(true);
      fdbCliBinary.setExecutable(true);
      fdbServerBinary.setExecutable(true);

      FileUtils.moveFileToDirectory(fdbCSo, foundationLocalDir, false);
      FileUtils.moveFileToDirectory(fdbCliBinary, foundationLocalDir, false);
      FileUtils.moveFileToDirectory(fdbServerBinary, foundationLocalDir, false);

      // Set FDB_LIBRARY_PATH_FDB_C environment property to path of FDB_C_SO
      final String fdbCSoPath = foundationLocalDir.getAbsolutePath() + "/" + FDB_C_SO;
      System.setProperty(FDB_C_LIBRARY_PATH, fdbCSoPath);
    }

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

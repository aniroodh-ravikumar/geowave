package org.locationtech.geowave.datastore.foundationdb.util;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.FDBException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.concurrent.Semaphore;
import org.iq80.leveldb.WriteBatch;

abstract public class AbstractFoundationDBTable {

  protected final short adapterId;
  private Database db;
  protected boolean visibilityEnabled;

  // Batch Write Fields
  // TODO: Figure out if FoundationDB supports batch writes
  private WriteBatch currentBatch;
  private final int batchSize;
  protected boolean compactOnWrite;
  private final boolean batchWrite;

  public AbstractFoundationDBTable(
      final short adapterId,
      final boolean visibilityEnabled,
      final boolean compactOnWrite,
      final int batchSize) {
    super();
    this.adapterId = adapterId;
    this.visibilityEnabled = visibilityEnabled;
    this.compactOnWrite = compactOnWrite;
    this.batchSize = batchSize;
    batchWrite = batchSize > 1;
  }

  public void delete(final byte[] key) {
    Database db = getDb();
    db.run(tr -> {
      tr.clear(key);
      return null;
    });
  }

  @SuppressFBWarnings(
      justification = "The null check outside of the synchronized block is intentional to minimize the need for synchronization.")
  protected void put(final byte[] key, final byte[] value) {
    // TODO: Handle batchWrites if FoundationDB supports this
    Database db = getDb();
    db.run(tr -> {
      tr.set(key, value);
      return null;
    });
  }

  private void flushWriteQueue() {}

  @SuppressFBWarnings(
      justification = "The null check outside of the synchronized block is intentional to minimize the need for synchronization.")
  public void flush() {}

  protected void internalFlush() {}

  public void compact() {}

  private void waitForBatchWrite() {}

  public void close() {}

  @SuppressFBWarnings(
      justification = "double check for null is intentional to avoid synchronized blocks when not needed.")
  protected Database getDb() {
    if (db == null) {
      FDB fdb = FDB.selectAPIVersion(610);
      db = fdb.open();
    }
    return db;
  }

  // TODO: Figure out if FoundationDB supports batch writes
  private static class BatchWriter implements Runnable {
    private final WriteBatch dataToWrite;
    private final FDB db;
    // private final WriteOptions options;
    private final Semaphore writeSemaphore;

    private BatchWriter(
        final WriteBatch dataToWrite,
        final FDB db,
        // final WriteOptions options,
        final Semaphore writeSemaphore) {
      super();
      this.dataToWrite = dataToWrite;
      this.db = db;
      // this.options = options;
      this.writeSemaphore = writeSemaphore;
    }

    @Override
    public void run() {
      try {
        // db.write(options, dataToWrite);
        // dataToWrite.close();
      } catch (final FDBException e) {
        // LOGGER.warn("Unable to write batch", e);
      } finally {
        writeSemaphore.release();
      }
    }
  }

}

package org.locationtech.geowave.datastore.foundationdb.util;

import com.apple.foundationdb.FDB;
import com.apple.foundationdb.FDBException;
import com.google.common.util.concurrent.MoreExecutors;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.iq80.leveldb.WriteBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;

abstract public class AbstractFoundationDBTable {

  public AbstractFoundationDBTable() {
    super();
  }

  public void delete(final byte[] key) {

  }

  @SuppressFBWarnings(
      justification = "The null check outside of the synchronized block is intentional to minimize the need for synchronization.")
  protected void put(final byte[] key, final byte[] value) {}

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
  protected FDB getWriteDb() {
    return null;
  }

  @SuppressFBWarnings(
      justification = "double check for null is intentional to avoid synchronized blocks when not needed.")
  protected FDB getReadDb() {
    return null;
  }

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

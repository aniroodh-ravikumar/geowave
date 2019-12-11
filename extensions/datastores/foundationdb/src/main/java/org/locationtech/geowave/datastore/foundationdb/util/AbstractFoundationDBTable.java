package org.locationtech.geowave.datastore.foundationdb.util;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.KeyValue;
import com.google.common.util.concurrent.MoreExecutors;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.ArrayList;
import java.util.concurrent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides an interface for dealing with a FoundationDB table. Any code that needs to perform table operations should
 * use an instance of this class, rather than calling FDB operations directly. This class is thread-safe.
 */
abstract public class AbstractFoundationDBTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractFoundationDBTable.class);
  private static final int BATCH_WRITE_THREAD_SIZE = 16;
  private static final ExecutorService BATCH_WRITE_THREADS =
      MoreExecutors.getExitingExecutorService(
          (ThreadPoolExecutor) Executors.newFixedThreadPool(BATCH_WRITE_THREAD_SIZE));
  private static final int MAX_CONCURRENT_WRITE = 100;
  // only allow so many outstanding async reads or writes, use this semaphore
  // to control it
  private final Object BATCH_WRITE_MUTEX = new Object();
  private final Semaphore writeSemaphore = new Semaphore(MAX_CONCURRENT_WRITE);

  protected final short adapterId;
  private Database db;
  protected boolean visibilityEnabled;

  // Batch Write Fields
  private ArrayList<KeyValue> currentBatch;
  private final int batchSize;
  protected boolean compactOnWrite;
  private final boolean batchWrite;
  protected boolean readerDirty = false;

  /**
   * Create a FDB table using a given FDB instance. Batch-writes can be enabled if needed.
   *
   * Preconditions:
   *
   * <ul>
   *     <li>The FDB instance must not be closed.</li>
   * </ul>
   *
   * @param adapterId TODO
   * @param visibilityEnabled TODO
   * @param batchSize The number of rows to write at a time. If this is greater than 1, the table is created with
   *                  batch-write mode enabled. Otherwise, every write will immediately be persisted to the DB. This
   *                  field may not be less than 1.
   * @param db The FDB instance.
   */
  public AbstractFoundationDBTable(
      final short adapterId,
      final boolean visibilityEnabled,
      final int batchSize,
      final Database db) {
    super();
    this.adapterId = adapterId;
    this.visibilityEnabled = visibilityEnabled;
    this.batchSize = batchSize;
    batchWrite = batchSize > 1;
    this.db = db;
  }

  /**
   * Delete an entry from the table, identified by the key. The deletion is flushed immediately.
   *
   * Preconditions:
   *
   * <ul>
   *     <li>The table must not be closed.</li>
   * </ul>
   *
   * @param key The key.
   */
  public void delete(final byte[] key) {
    Database db = getDb();
    readerDirty = true;
    db.run(tr -> {
      tr.clear(key);
      return null;
    });
  }

  /**
   * Write a key-value pair to the table. If the table is in batch-write mode, the write may not be persisted
   * immediately - that will happen when the write queue is flushed, which occurs when its size reaches the batch write
   * size. If the table is not in batch-write mode (equivalent to the batch write size being 1), the write is
   * persisted immediately.
   *
   * Preconditions:
   *
   * <ul>
   *     <li>The table must not be closed.</li>
   * </ul>
   *
   * @param key The key.
   * @param value The value.
   */
  @SuppressFBWarnings(
      justification = "The null check outside of the synchronized block is intentional to minimize the need for synchronization.")
  protected void put(final byte[] key, final byte[] value) {
    Database db = getDb();
    if (batchWrite) {
      ArrayList<KeyValue> thisBatch = currentBatch;
      if (thisBatch == null) {
        synchronized (BATCH_WRITE_MUTEX) {
          if (currentBatch == null) {
            currentBatch = new ArrayList<>();
          }
          thisBatch = currentBatch;
        }
      }
      try {
        KeyValue keyValue = new KeyValue(key, value);
        thisBatch.add(keyValue);
      } catch (final FDBException e) {
        LOGGER.warn("Unable to add data to batched write", e);
      }
      if (thisBatch.size() >= batchSize) {
        synchronized (BATCH_WRITE_MUTEX) {
          if (currentBatch != null) {
            flushWriteQueue();
          }
        }
      }
    } else {
      readerDirty = true;
      db.run(tr -> {
        tr.set(key, value);
        return null;
      });
    }
  }

  /**
   * Flush the write queue, persisting all pending writes.
   */
  private void flushWriteQueue() {
    try {
      writeSemaphore.acquire();
      readerDirty = true;
      CompletableFuture.runAsync(
          new BatchWriter(currentBatch, getDb(), writeSemaphore),
          BATCH_WRITE_THREADS);
    } catch (final InterruptedException e) {
      LOGGER.warn("async write semaphore interrupted", e);
      writeSemaphore.release();
    }
    currentBatch = null;
  }

  /**
   * Flush any pending writes, and close the database if reads are dirty (i.e. if any writes or deletes have occurred).
   */
  @SuppressFBWarnings(
      justification = "The null check outside of the synchronized block is intentional to minimize the need for synchronization.")
  public void flush() {
    if (batchWrite) {
      synchronized (BATCH_WRITE_MUTEX) {
        if (currentBatch != null) {
          flushWriteQueue();
        }
        waitForBatchWrite();
      }
    }
    internalFlush();
  }

  protected void internalFlush() {
    // force re-opening a reader to catch the updates from this write
    if (readerDirty && (db != null)) {
      synchronized (this) {
        if (db != null) {
          db.close();
          db = null;
        }
      }
    }
  }

  private void waitForBatchWrite() {
    if (batchWrite) {
      // need to wait for all asynchronous batches to finish writing
      // before exiting close() method
      try {
        writeSemaphore.acquire(MAX_CONCURRENT_WRITE);
      } catch (final InterruptedException e) {
        LOGGER.warn("Unable to wait for batch write to complete");
      }
      writeSemaphore.release(MAX_CONCURRENT_WRITE);
    }
  }

  public void close() {
    waitForBatchWrite();
    synchronized (this) {
      if (db != null) {
        db.close();
        db = null;
      }
    }
  }

  /**
   * Get the associated {@link Database} instance.
   *
   * @return The current FDB instance.
   */
  protected Database getDb() {
    return this.db;
  }

  private static class BatchWriter implements Runnable {
    private final ArrayList<KeyValue> dataToWrite;
    private final Database db;
    private final Semaphore writeSemaphore;

    private BatchWriter(
        final ArrayList<KeyValue> dataToWrite,
        final Database db,
        final Semaphore writeSemaphore) {
      super();
      this.dataToWrite = dataToWrite;
      this.db = db;
      this.writeSemaphore = writeSemaphore;
    }

    @Override
    public void run() {
      try {
        db.run(tr -> {
          for (KeyValue keyValue : dataToWrite) {
            tr.set(keyValue.getKey(), keyValue.getValue());
          }
          return null;
        });
      } catch (final FDBException e) {
        LOGGER.warn("Unable to write batch", e);
      } finally {
        writeSemaphore.release();
      }
    }
  }

}

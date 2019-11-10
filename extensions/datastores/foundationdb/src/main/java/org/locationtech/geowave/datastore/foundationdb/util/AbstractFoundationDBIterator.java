package org.locationtech.geowave.datastore.foundationdb.util;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.async.AsyncIterator;
import org.locationtech.geowave.core.store.CloseableIterator;

import java.util.NoSuchElementException;
import java.util.concurrent.CompletionException;

/**
 * This iterator handles timeout exceptions that FDB throws if the range we're currently reading is older than a certain
 * time limit. Whenever that happens, this class just creates another range iterator from the next-to-be-read index to
 * the last index.
 *
 * Note that this iterator is not guaranteed to return consistent data, since the data may be loaded in multiple
 * transactions.
 */
public abstract class AbstractFoundationDBIterator<T> implements CloseableIterator<T> {
  protected final Database db;
  protected boolean closed = false;
  protected AsyncIterator<KeyValue> it;

  // The end of the range
  private final byte[] end;
  // The next value that we want to read
  private final byte[] next;

  public AbstractFoundationDBIterator(Database db, byte[] start, byte[] end) {
    super();
    this.db = db;
    this.end = end;
    this.next = new byte[start.length];
    System.arraycopy(start, 0, next, 0, start.length);
  }

  @Override
  public synchronized boolean hasNext() {
    return !closed && it.hasNext();
  }

  @Override
  public synchronized T next() {
    if (closed) {
      throw new NoSuchElementException();
    }
    try {
      // TODO: maybe we should keep a counter and batch-increment when we load?
      increment(next);
      return readRow(it.next());
    } catch (CompletionException e) {
      // Try to load the next piece
      it = db.run(tr -> tr.getRange(next, end)).iterator();
      return readRow(it.next());
    }
  }

  protected abstract T readRow(KeyValue keyValue);

  @Override
  public synchronized void close() {
    closed = true;
  }

  private byte[] increment(byte[] start) {
    // TODO
    return start;
  }
}

package org.locationtech.geowave.datastore.foundationdb.util;

import com.apple.foundationdb.KeyValue;
import org.locationtech.geowave.core.store.CloseableIterator;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Provides an interface for dealing with a FoundationDB iterator.
 */
public abstract class AbstractFoundationDBIterator<T> implements CloseableIterator<T> {
  protected boolean closed = false;
  protected Iterator<KeyValue> it;

  public AbstractFoundationDBIterator(final Iterator<KeyValue> it) {
    super();
    this.it = it;
  }

  /**
   * Check if there are more elements in the iterator.
   *
   * @return Return true if closed is set to false and there exists another key-value pair in the iterator, and false otherwise.
   *
   */
  @Override
  public boolean hasNext() {
    return !closed && it.hasNext();
  }

  /**
   * Get the next element in the iterator.
   *
   * Preconditions:
   *
   * <ul>
   * <li> hasNext must have returned true before calling this method.</li>
   * </ul>
   *
   * @return Return the key-value pair in the iterator.
   *
   */
  @Override
  public T next() {
    if (closed) {
      throw new NoSuchElementException();
    }
    return readRow(it.next());
  }

  protected abstract T readRow(KeyValue keyValue);

    /**
     * Close this iterator.
     *
     * Post-conditions:
     *
     * <ul>
     * <li> hasNext will return false once this method is called.</li>
     * </ul>
     *
     */
  @Override
  public void close() {
    closed = true;
  }
}


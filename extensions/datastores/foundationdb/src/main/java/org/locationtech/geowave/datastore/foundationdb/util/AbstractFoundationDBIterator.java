package org.locationtech.geowave.datastore.foundationdb.util;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.async.AsyncIterator;
import org.locationtech.geowave.core.store.CloseableIterator;
import java.util.NoSuchElementException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractFoundationDBIterator<T> implements CloseableIterator<T> {
  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractFoundationDBIterator.class);
  protected boolean closed = false;
  protected AsyncIterator<KeyValue> it;

  public AbstractFoundationDBIterator(final AsyncIterator<KeyValue> it) {
    super();
    this.it = it;
  }

  @Override
  public boolean hasNext() {
    LOGGER.warn("IN HAS NEXT");
    return !closed && it.hasNext();
  }

  @Override
  public T next() {
    if (closed) {
      throw new NoSuchElementException();
    }
    return readRow(it.next());
  }

  protected abstract T readRow(KeyValue keyValue);

  @Override
  public void close() {
    closed = true;
  }
}

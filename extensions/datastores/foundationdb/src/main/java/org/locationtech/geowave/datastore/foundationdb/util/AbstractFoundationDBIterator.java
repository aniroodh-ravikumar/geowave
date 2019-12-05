package org.locationtech.geowave.datastore.foundationdb.util;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterator;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterator;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import com.apple.foundationdb.KeyValue;
import org.locationtech.geowave.core.store.CloseableIterator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletionException;

public abstract class AbstractFoundationDBIterator<T> implements CloseableIterator<T> {
  protected boolean closed = false;
  protected Iterator<KeyValue> it;

  public AbstractFoundationDBIterator(final Iterator<KeyValue> it) {
    super();
    this.it = it;
  }

  @Override
  public boolean hasNext() {
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


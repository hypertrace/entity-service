package org.hypertrace.entity;

import java.util.Iterator;
import org.hypertrace.core.documentstore.CloseableIterator;

public class TestUtils {

  public static <T> CloseableIterator<T> convertToCloseableIterator(Iterator<T> iterator) {
    return new CloseableIterator<>() {
      @Override
      public void close() {}

      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public T next() {
        return iterator.next();
      }
    };
  }
}

package tachyon.worker;

import java.io.Closeable;
import java.io.IOException;

/**
 * Defines how to interact with a server running the data protocol.
 */
public interface DataServer extends Closeable {
  int getPort();

  boolean isClosed();

  void close() throws IOException;
}

package tachyon.worker;

import java.io.IOException;

public abstract class DataServer implements Runnable {
  public abstract void close() throws IOException;

  public abstract boolean isClosed();
}

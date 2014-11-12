package tachyon.worker;

import java.io.Closeable;
import java.net.InetSocketAddress;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import tachyon.conf.WorkerConf;
import tachyon.util.CommonUtils;

/**
 * Defines how to interact with a server running the data protocol.
 */
public abstract class DataServer implements Closeable {

  public static DataServer createDataServer(final InetSocketAddress dataAddress,
      final BlocksLocker blockLocker) {
    try {
      Object dataServerObj =
          CommonUtils.createNewClassInstance(WorkerConf.get().DATA_SERVER_CLASS,
              new Class[] { InetSocketAddress.class, BlocksLocker.class },
              new Object[] { dataAddress, blockLocker });
      Preconditions.checkArgument(dataServerObj instanceof DataServer,
          "Data Server is not configured properly.");
      return (DataServer) dataServerObj;
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public abstract int getPort();

  public abstract boolean isClosed();
}

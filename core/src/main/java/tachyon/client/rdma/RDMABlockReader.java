package tachyon.client.rdma;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;

import org.apache.log4j.Logger;

import com.mellanox.jxio.jxioConnection.JxioConnection;

import tachyon.Constants;
import tachyon.NetworkType;
import tachyon.client.RemoteBlockReader;
import tachyon.conf.UserConf;
import tachyon.util.NetworkUtils;
import tachyon.worker.DataServerMessage;

public class RDMABlockReader implements RemoteBlockReader {

  private static final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);
  private final UserConf USER_CONF = UserConf.get();

  @Override
  public ByteBuffer readRemoteBlock(String host, int port, long blockId, long offset, long length)
      throws IOException {
    URI uri =
        NetworkUtils.createRdmaUri(USER_CONF.NETWORK_TYPE, host, port, blockId, offset, length);
    JxioConnection jc = new JxioConnection(uri);
    try {
      jc.setRcvSize(655360); // 10 buffers in msg pool
      InputStream input = jc.getInputStream();
      LOG.info("Connected to remote machine " + uri);

      DataServerMessage recvMsg = DataServerMessage.createBlockResponseMessage(false, blockId);
      recvMsg.recv(input);
      LOG.info("Data " + blockId + " from remote machine " + host + ":" + port + " received");

      if (!recvMsg.isMessageReady()) {
        LOG.info("Data " + blockId + " from remote machine is not ready.");
        return null;
      }

      if (recvMsg.getBlockId() < 0) {
        LOG.info("Data " + recvMsg.getBlockId() + " is not in remote machine.");
        return null;
      }

      return recvMsg.getReadOnlyData();

    } finally {
      jc.disconnect();
    }
  }
}

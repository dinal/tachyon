package tachyon.client.rdma;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.accelio.jxio.jxioConnection.JxioConnection;

import tachyon.Constants;
import tachyon.client.RemoteBlockReader;
import tachyon.conf.UserConf;
import tachyon.worker.DataServerMessage;

public class RDMARemoteBlockReader implements RemoteBlockReader {

  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private static String sNetwork = setNetwork();
  private final String mUriFormat = sNetwork + "://%s:%d/blockId=%d&offset=%d&length=%d";

  @Override
  public ByteBuffer readRemoteBlock(String host, int port, long blockId, long offset, long length)
      throws IOException {
    try {
      URI uri = new URI(String.format(mUriFormat, host, port, blockId, offset, length));
      JxioConnection jc = new JxioConnection(uri);
      try {
        jc.setRcvSize(Constants.CLIENT_MSGPOOL_SIZE); // 10 buffers in msg pool
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
    } catch (URISyntaxException e) {
      throw new IOException("rdma uri could not be resolved");
    }
  }
  
  public static String setNetwork() {
    String net = System.getProperty("tachyon.jxio.network");
    if (net == null) {
      net = "rdma";
    }
    return net;
  }
}

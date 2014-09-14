package tachyon.client;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface RemoteBlockReader {

  ByteBuffer readRemoteBlock(String host, int port, long blockId, long offset, long length)
      throws IOException;
}

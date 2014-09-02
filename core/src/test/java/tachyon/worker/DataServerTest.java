/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.worker;

import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.SocketChannel;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.mellanox.jxio.jxioConnection.JxioConnection;

import tachyon.TestUtils;
import tachyon.client.TachyonFS;
import tachyon.client.WriteType;
import tachyon.conf.UserConf;
import tachyon.master.LocalTachyonCluster;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.InvalidPathException;
import tachyon.util.NetworkUtils;
import tachyon.worker.DataServerMessage;

/**
 * Unit tests for tachyon.DataServer.
 */
public class DataServerTest {
  private final int WORKER_CAPACITY_BYTES = 1000;
  private final int USER_QUOTA_UNIT_BYTES = 100;
  private final UserConf USER_CONF = UserConf.get();
  private LocalTachyonCluster mLocalTachyonCluster = null;
  private TachyonFS mTFS = null;

  @After
  public final void after() throws Exception {
    mLocalTachyonCluster.stop();
    System.clearProperty("tachyon.user.quota.unit.bytes");
  }

  @Before
  public final void before() throws IOException {
    System.setProperty("tachyon.user.quota.unit.bytes", USER_QUOTA_UNIT_BYTES + "");
    mLocalTachyonCluster = new LocalTachyonCluster(WORKER_CAPACITY_BYTES);
    mLocalTachyonCluster.start();
    mTFS = mLocalTachyonCluster.getClient();
  }

  @Test
  public void readPartialTest1() throws InvalidPathException, FileAlreadyExistException,
      IOException {
    int fileId = TestUtils.createByteFile(mTFS, "/testFile", WriteType.MUST_CACHE, 10);
    long blockId = mTFS.getBlockId(fileId, 0);
    DataServerMessage recvMsg = DataServerMessage.createBlockResponseMessage(false, blockId, 0, 6);
    if (USER_CONF.NETWORK_TYPE.equals("rdma")) {
      URI uri =
          NetworkUtils.createRdmaUri(mTFS.getFileBlocks(fileId).get(0).getLocations().get(0).mHost,
              mTFS.getFileBlocks(fileId).get(0).getLocations().get(0).mPort + 1, blockId, 0, 6);
      if (uri != null) {
        JxioConnection jc = new JxioConnection(uri);
        jc.setRcvSize(655360); // 10 buffers in msg pool
        InputStream input = jc.getInputStream();
        recvMsg.recv(input);
        jc.disconnect();
      }
    } else {
      DataServerMessage sendMsg;
      sendMsg = DataServerMessage.createBlockRequestMessage(blockId, 0, 6);
      SocketChannel socketChannel =
          SocketChannel.open(new InetSocketAddress(mTFS.getFileBlocks(fileId).get(0).getLocations()
              .get(0).mHost, mTFS.getFileBlocks(fileId).get(0).getLocations().get(0).mPort + 1));
      while (!sendMsg.finishSending()) {
        sendMsg.send(socketChannel);
      }
      while (!recvMsg.isMessageReady()) {
        int numRead = recvMsg.recv(socketChannel);
        if (numRead == -1) {
          break;
        }
      }
      socketChannel.close();
    }
    Assert.assertEquals(TestUtils.getIncreasingByteBuffer(6), recvMsg.getReadOnlyData());
  }

  @Test
  public void readPartialTest2() throws InvalidPathException, FileAlreadyExistException,
      IOException {
    int fileId = TestUtils.createByteFile(mTFS, "/testFile", WriteType.MUST_CACHE, 10);
    long blockId = mTFS.getBlockId(fileId, 0);
    DataServerMessage recvMsg = DataServerMessage.createBlockResponseMessage(false, blockId, 2, 6);
    if (USER_CONF.NETWORK_TYPE.equals("rdma")) {
      URI uri =
          NetworkUtils.createRdmaUri(mTFS.getFileBlocks(fileId).get(0).getLocations().get(0).mHost,
              mTFS.getFileBlocks(fileId).get(0).getLocations().get(0).mPort + 1, blockId, 0, 6);
      if (uri != null) {
        JxioConnection jc = new JxioConnection(uri);
        jc.setRcvSize(655360); // 10 buffers in msg pool
        InputStream input = jc.getInputStream();
        recvMsg.recv(input);
        jc.disconnect();
      }
    } else {
      DataServerMessage sendMsg;
      sendMsg = DataServerMessage.createBlockRequestMessage(blockId, 2, 6);
      SocketChannel socketChannel =
          SocketChannel.open(new InetSocketAddress(mTFS.getFileBlocks(fileId).get(0).getLocations()
              .get(0).mHost, mTFS.getFileBlocks(fileId).get(0).getLocations().get(0).mPort + 1));
      while (!sendMsg.finishSending()) {
        sendMsg.send(socketChannel);
      }

      while (!recvMsg.isMessageReady()) {
        int numRead = recvMsg.recv(socketChannel);
        if (numRead == -1) {
          break;
        }
      }
      socketChannel.close();
    }
    Assert.assertEquals(TestUtils.getIncreasingByteBuffer(2, 6), recvMsg.getReadOnlyData());
  }

  @Test
  public void readTest() throws InvalidPathException, FileAlreadyExistException, IOException {
    int fileId = TestUtils.createByteFile(mTFS, "/testFile", WriteType.MUST_CACHE, 10);
    long blockId = mTFS.getBlockId(fileId, 0);
    DataServerMessage recvMsg = DataServerMessage.createBlockResponseMessage(false, blockId);
    if (USER_CONF.NETWORK_TYPE.equals("rdma")) {
      URI uri =
          NetworkUtils.createRdmaUri(mTFS.getFileBlocks(fileId).get(0).getLocations().get(0).mHost,
              mTFS.getFileBlocks(fileId).get(0).getLocations().get(0).mPort + 1, blockId, 0, 6);
      if (uri != null) {
        JxioConnection jc = new JxioConnection(uri);
        jc.setRcvSize(655360); // 10 buffers in msg pool
        InputStream input = jc.getInputStream();
        recvMsg.recv(input);
        jc.disconnect();
      }
    } else {
      DataServerMessage sendMsg = DataServerMessage.createBlockRequestMessage(blockId);
      SocketChannel socketChannel =
          SocketChannel.open(new InetSocketAddress(mTFS.getFileBlocks(fileId).get(0).getLocations()
              .get(0).mHost, mTFS.getFileBlocks(fileId).get(0).getLocations().get(0).mPort + 1));
      while (!sendMsg.finishSending()) {
        sendMsg.send(socketChannel);
      }
      while (!recvMsg.isMessageReady()) {
        int numRead = recvMsg.recv(socketChannel);
        if (numRead == -1) {
          break;
        }
      }
      socketChannel.close();
    }
    Assert.assertEquals(TestUtils.getIncreasingByteBuffer(10), recvMsg.getReadOnlyData());
  }
}

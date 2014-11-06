package tachyon.worker.rdma;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import com.google.common.base.Throwables;
import org.apache.log4j.Logger;

import org.accelio.jxio.EventName;
import org.accelio.jxio.EventQueueHandler;
import org.accelio.jxio.EventReason;
import org.accelio.jxio.Msg;
import org.accelio.jxio.MsgPool;
import org.accelio.jxio.ServerPortal;
import org.accelio.jxio.ServerSession;
import org.accelio.jxio.ServerSession.SessionKey;
import org.accelio.jxio.WorkerCache.Worker;
import org.accelio.jxio.exceptions.JxioGeneralException;
import org.accelio.jxio.exceptions.JxioSessionClosedException;

import tachyon.Constants;
import tachyon.NetworkType;
import tachyon.conf.WorkerConf;
import tachyon.worker.BlocksLocker;
import tachyon.worker.DataServer;
import tachyon.worker.DataServerMessage;

public class RDMADataServer implements Runnable, DataServer {

  private static final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);
  // The blocks locker manager.
  private final BlocksLocker mBlocksLocker;
  private final EventQueueHandler eqh;
  private final ServerPortal listener;
  private ArrayList<MsgPool> msgPools = new ArrayList<MsgPool>();
  private final Thread mListenerThread;

  public RDMADataServer(InetSocketAddress address, BlocksLocker locker) {
    LOG.info("Starting RDMADataServer @ " + address.toString());
    URI uri = constructRdmaServerUri(address.getHostName(), address.getPort());
    mBlocksLocker = locker;
    MsgPool pool =
        new MsgPool(Constants.SERVER_INITIAL_BUF_COUNT, 0,
            org.accelio.jxio.jxioConnection.Constants.MSGPOOL_BUF_SIZE);
    msgPools.add(pool);
    eqh =
        new EventQueueHandler(new EqhCallbacks(Constants.SERVER_INC_BUF_COUNT, 0,
            org.accelio.jxio.jxioConnection.Constants.MSGPOOL_BUF_SIZE));
    eqh.bindMsgPool(pool);
    listener = new ServerPortal(eqh, uri, new PortalServerCallbacks(), null);
    mListenerThread = new Thread(this);
    mListenerThread.start();
  }

  /**
   * Callbacks for the listener server portal
   */
  public class PortalServerCallbacks implements ServerPortal.Callbacks {

    public void onSessionEvent(EventName session_event, EventReason reason) {
      LOG.debug("got event " + session_event.toString() + "because of " + reason.toString());
      if (session_event == EventName.PORTAL_CLOSED) {
        eqh.breakEventLoop();
      }
    }

    public void onSessionNew(SessionKey sesKey, String srcIP, Worker workerHint) {
      LOG.info("onSessionNew " + sesKey.getUri());
      SessionServerCallbacks callbacks = new SessionServerCallbacks(sesKey.getUri());
      ServerSession session = new ServerSession(sesKey, callbacks);
      callbacks.setSession(session);
      listener.accept(session);
    }
  }

  public class SessionServerCallbacks implements ServerSession.Callbacks {
    private final DataServerMessage responseMessage;
    private ServerSession session;

    public SessionServerCallbacks(String uri) {
      String[] params = uri.split("blockId=")[1].split("\\?")[0].split("&");
      long blockId = Long.parseLong(params[0]);
      long offset = Long.parseLong(params[1].split("=")[1]);
      long length = Long.parseLong(params[2].split("=")[1]);
      LOG.debug("got request for block id " + blockId + " with offset " + offset + " and length "
          + length);
      int lockId = mBlocksLocker.lock(blockId);
      responseMessage = DataServerMessage.createBlockResponseMessage(true, blockId, offset, length);
      responseMessage.setLockId(lockId);
    }

    public void setSession(ServerSession ses) {
      session = ses;
    }

    public void onRequest(Msg m) {
      if (session.getIsClosing()) {
        session.discardRequest(m);
      } else {
        responseMessage.copy(m.getOut());
        try {
          session.sendResponse(m);
        } catch (JxioGeneralException e) {
          LOG.error("Exception accured while sending messgae " + e.toString());
          session.discardRequest(m);
        } catch (JxioSessionClosedException e) {
          LOG.error("session was closed unexpectedly " + e.toString());
          session.discardRequest(m);
        }
      }

      if (!session.getIsClosing() && responseMessage.finishSending()) {
        session.close();
      }
    }

    public void onSessionEvent(EventName session_event, EventReason reason) {
      LOG.debug("got event " + session_event.toString() + ", the reason is " + reason.toString());
      if (session_event == EventName.SESSION_CLOSED) {
        responseMessage.close();
        mBlocksLocker.unlock(Math.abs(responseMessage.getBlockId()), responseMessage.getLockId());
      }
    }

    public boolean onMsgError(Msg msg, EventReason reason) {
      LOG.error(this.toString() + " onMsgErrorCallback. reason is " + reason);
      return true;
    }
  }

  @Override
  public void run() {
    int ret = eqh.runEventLoop(-1, -1);
    if (ret == -1) {
      LOG.error(this.toString() + " exception occurred in eventLoop:" + eqh.getCaughtException());
    }
    eqh.stop();
    eqh.close();
    for (MsgPool mp : msgPools) {
      mp.deleteMsgPool();
    }
    msgPools.clear();
  }

  @Override
  public void close() {
    LOG.info("closing server");
    eqh.breakEventLoop();
  }

  @Override
  public boolean isClosed() {
    return listener.getIsClosing();
  }

  class EqhCallbacks implements EventQueueHandler.Callbacks {
    private final RDMADataServer outer = RDMADataServer.this;
    private final int numMsgs;
    private final int inMsgSize;
    private final int outMsgSize;

    public EqhCallbacks(int msgs, int in, int out) {
      numMsgs = msgs;
      inMsgSize = in;
      outMsgSize = out;
    }

    public MsgPool getAdditionalMsgPool(int in, int out) {
      MsgPool mp = new MsgPool(numMsgs, inMsgSize, outMsgSize);
      LOG.warn(this.toString() + " " + outer.toString() + ": new MsgPool: " + mp);
      outer.msgPools.add(mp);
      return mp;
    }
  }

  @Override
  public int getPort() {
    return listener.getUri().getPort();
  }

  private URI constructRdmaServerUri(String host, int port) {
    URI uri;
    try {
      if (WorkerConf.get().NETWORK_TYPE == NetworkType.RDMA) {
        uri = new URI("rdma://" + host + ":" + port);
      } else {
        uri = new URI("tcp://" + host + ":" + port);
      }
      return uri;
    } catch (URISyntaxException e) {
      LOG.error("could not resolve rdma data server uri, NetworkType is "
          + WorkerConf.get().NETWORK_TYPE);
      throw Throwables.propagate(e);
    }
  }
}

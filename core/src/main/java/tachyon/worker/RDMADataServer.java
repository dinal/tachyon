package tachyon.worker;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import com.mellanox.jxio.EventName;
import com.mellanox.jxio.EventQueueHandler;
import com.mellanox.jxio.EventReason;
import com.mellanox.jxio.Msg;
import com.mellanox.jxio.MsgPool;
import com.mellanox.jxio.ServerPortal;
import com.mellanox.jxio.ServerSession;
import com.mellanox.jxio.ServerSession.SessionKey;
import com.mellanox.jxio.WorkerCache.Worker;
import com.mellanox.jxio.exceptions.JxioGeneralException;
import com.mellanox.jxio.exceptions.JxioSessionClosedException;

import tachyon.Constants;
import tachyon.Users;
import tachyon.util.CommonUtils;

public class RDMADataServer extends DataServer {

  private static final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);
  // The blocks locker manager.
  private final BlocksLocker mBlocksLocker;
  private final EventQueueHandler eqh;
  private final ServerPortal listener;
  private ArrayList<MsgPool> msgPools = new ArrayList<MsgPool>();
  private final int numMsgPoolBuffers = 500;
  private boolean waitingToClose = false;
  private boolean sessionClosed = false;

  public RDMADataServer(URI uri, WorkerStorage workerStorage) {
    LOG.info("Starting RDMADataServer @ " + uri.toString());
    mBlocksLocker = new BlocksLocker(workerStorage, Users.sDATASERVER_USER_ID);
    MsgPool pool = new MsgPool(numMsgPoolBuffers, 0, 64 * 1024);
    msgPools.add(pool);
    eqh = new EventQueueHandler(new EqhCallbacks(numMsgPoolBuffers, 0, 64 * 1024));
    eqh.bindMsgPool(pool);
    listener = new ServerPortal(eqh, uri, new PortalServerCallbacks(), null);
  }

  /**
   * Callbacks for the listener server portal
   */
  public class PortalServerCallbacks implements ServerPortal.Callbacks {

    public void onSessionEvent(EventName session_event, EventReason reason) {
      LOG.info("got event " + session_event.toString() + "because of " + reason.toString());
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
      long blockId = Long.parseLong(uri.split("blockId=")[1].split("\\?")[0]);
      LOG.info("got request for block id " + blockId);
      int lockId = mBlocksLocker.lock(blockId);
      responseMessage = DataServerMessage.createBlockResponseMessage(true, blockId, 0, -1);
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
          LOG.error("Exception accured while sending messgae "+e.toString());
        } catch (JxioSessionClosedException e) {
          LOG.error("session was closed unexpectedly "+e.toString());
        }
      }

      if (!session.getIsClosing() && responseMessage.finishSending()) {
        LOG.info("finished reading, closing session");
        session.close();
      }
    }

    public void onSessionEvent(EventName session_event, EventReason reason) {
      LOG.info("got event " + session_event.toString() + ", the reason is " + reason.toString());
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
    LOG.info("Server Run");
    eqh.runEventLoop(-1, -1);
    LOG.info("Server Run Ended");
  }

  @Override
  public void close() throws IOException {
    LOG.info(this.toString() + " closing server");
    listener.close();
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
}

package tachyon;

public enum NetworkType {
  NIO, NETTY, RDMA, RDMA_TCP;

  public static boolean isRdma(NetworkType type) {
    return type == RDMA || type == RDMA_TCP;
  }
}

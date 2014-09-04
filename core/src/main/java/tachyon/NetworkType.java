package tachyon;

public enum NetworkType {
  TCP, RDMA, TCP_RDMA;

  public static boolean isRdma(NetworkType type) {
    return type == RDMA || type == TCP_RDMA;
  }
}
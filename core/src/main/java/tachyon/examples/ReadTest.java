package tachyon.examples;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.log4j.Logger;
import tachyon.Constants;
import tachyon.Version;
import tachyon.client.InStream;
import tachyon.client.OutStream;
import tachyon.client.ReadType;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;
import tachyon.client.WriteType;

public class ReadTest {

  private static Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);
  private static TachyonFS sTachyonClient;
  private static String sFilePath = null;
  private static String operation = null;
  private static long bytes = 0;
  private static boolean checkData = false;

  public static void main(String[] args) throws IOException {
    if (args.length < 4) {
      System.out
          .println("java -cp target/tachyon-"
              + Version.VERSION
              + "-jar-with-dependencies.jar "
              + "tachyon.examples.ReadTest <TachyonMasterAddress> <FilePath> <Operation> <FileBytes> <optional_checkData>");
      System.exit(-1);
    }
    sTachyonClient = TachyonFS.get(args[0]);
    sFilePath = args[1];
    operation = args[2];
    bytes = Long.parseLong(args[3]);
    if (args.length == 5) {
      checkData = Boolean.parseBoolean(args[4]);
    }

    if (operation.compareTo("read") == 0) {
      readFile();
    } else {
      createFile(sFilePath);
      writeFile();
    }

    System.exit(0);
  }

  public static void createFile(String path) throws IOException {
    int fileId = sTachyonClient.createFile(path);
    System.out.println("File " + path + " created with id " + fileId);
  }

  public static void writeFile() throws IOException {
    TachyonFile file = sTachyonClient.getFile(sFilePath);
    OutStream os = file.getOutStream(WriteType.MUST_CACHE);
    //
    for (long k = 0; k < bytes; k ++) {
      os.write((byte) k);
    }
    os.close();

    System.out.println("Write to file " + sFilePath + " " + bytes + " bytes");
  }

  public static void readFile() throws IOException {
    long time = 0;
    int read = 0;
    TachyonFile file = sTachyonClient.getFile(sFilePath);
    System.out.println("Going to read file " + sFilePath + " size " + file.length());
    time = System.nanoTime();
    InStream is = file.getInStream(ReadType.NO_CACHE);
    if (checkData) {
      LOG.info("validating data");
      long k = 0;
      time = System.nanoTime();
      while (read != -1) {
        read = is.read();
        if (read != -1 && (byte) read != (byte) k) {
          LOG.error("ERROR IN TEST, got " + (byte) read + " expected " + (byte) k);
          return;
        }
        k ++;
      }
    } else {
      byte[] buf = new byte[4 * 1024];
      while (read != -1) {
        read = is.read(buf);
      }
    }
    time = System.nanoTime() - time;
    System.out.println("Finished reading file " + sFilePath + " time " + time+" ");
  }
}

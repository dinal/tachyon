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
  private static String operation = null;
  private static long bytes = 0;
  private static boolean checkData = false;
  private static String[] files = null;

  public static void main(String[] args) throws IOException {
    if (args.length < 4) {
      System.out
          .println("java -cp target/tachyon-"
              + Version.VERSION
              + "-jar-with-dependencies.jar "
              + "tachyon.examples.ReadTest <TachyonMasterAddress> <Operation> <FileList_CommaSeparated> <FileBytes> <Optional_CheckData>");
      System.exit(-1);
    }
    sTachyonClient = TachyonFS.get(args[0]);
    operation = args[1];
    files = args[2].split(",");
    bytes = Long.parseLong(args[3]);
    if (args.length == 5) {
      checkData = Boolean.parseBoolean(args[4]);
    }

    LOG.info("Going to " + operation + " " + files.length + " files with " + bytes + " bytes, "
        + args[2]);
    if (operation.equals("read")) {
      readFiles();
    } else {
      createFiles();
      writeFiles();
    }

    System.exit(0);
  }

  public static void createFiles() throws IOException {
    for (int i = 0; i < files.length; i ++) {
      int fileId = sTachyonClient.createFile(files[i]);
      LOG.info("File " + files[i] + " created with id " + fileId);
    }
  }

  public static void writeFiles() throws IOException {
    for (int i = 0; i < files.length; i ++) {
      TachyonFile file = sTachyonClient.getFile(files[i]);
      OutStream os = file.getOutStream(WriteType.MUST_CACHE);
      //
      for (long k = 0; k < bytes; k ++) {
        os.write((byte) k);
      }
      os.close();

      LOG.info("Write to file " + files[i] + " " + bytes + " bytes");
    }
  }

  public static void readFiles() throws IOException {
    try {
      for (int i = 0; i < files.length; i ++) {
        long time = 0;
        int read = 0;
        TachyonFile file = sTachyonClient.getFile(files[i]);
        LOG.info("Going to read file " + files[i] + " size " + file.length());
        time = System.nanoTime();
        InStream is = file.getInStream(ReadType.NO_CACHE);
        if (checkData) {
          LOG.info("validating data");
          long k = 0;
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
        System.out.println("Finished reading file " + files[i] + " time " + time + " \n");
      }
    } catch (Exception e) {
      LOG.error("got Exception is test " + e.toString());
    }
  }
}

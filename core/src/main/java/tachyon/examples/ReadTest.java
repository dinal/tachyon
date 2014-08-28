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

	private static Logger    LOG       = Logger.getLogger(Constants.LOGGER_TYPE);

	private static TachyonFS sTachyonClient;
	private static String    sFilePath = null;
	private static String    operation = null;
	private static int       bytes     = 0;

	public static void main(String[] args) throws IOException {
		if (args.length != 4) {
			System.out.println("java -cp target/tachyon-" + Version.VERSION + "-jar-with-dependencies.jar "
			        + "tachyon.examples.ReadTest <TachyonMasterAddress> <FilePath> <Operation> <FileBytes>");
			System.exit(-1);
		}
		sTachyonClient = TachyonFS.get(args[0]);
		sFilePath = args[1];
		operation = args[2];
		bytes = Integer.parseInt(args[3]);

		if (operation.compareTo("read") == 0) {
			readFile();
		} else {
			createFile();
			writeFile();
		}

		System.exit(0);
	}

	public static void createFile() throws IOException {
		int fileId = sTachyonClient.createFile(sFilePath);
		System.out.println("File " + sFilePath + " created with id " + fileId);
	}

	public static void writeFile() throws IOException {
		TachyonFile file = sTachyonClient.getFile(sFilePath);
		OutStream os = file.getOutStream(WriteType.MUST_CACHE);
		ByteBuffer sRawData = ByteBuffer.allocate(bytes);
		sRawData.order(ByteOrder.nativeOrder());
		for (int k = 0; k < bytes; k++) {
			sRawData.array()[k] = (byte) k;
		}
		os.write(sRawData.array());
		os.close();

		System.out.println("Write to file " + sFilePath + " " + bytes + " bytes");
	}

	public static void readFile() throws IOException {
		TachyonFile file = sTachyonClient.getFile(sFilePath);
		InStream is = file.getInStream(ReadType.NO_CACHE);
		byte[] buf = new byte[4*1024];
		int read = 0;
		System.out.println("Going to read file " + sFilePath+" size "+file.length());
		long time = System.nanoTime();
		while (read != -1) {
			read = is.read(buf);
		}
		time = System.nanoTime() - time;
		System.out.println("Finished reading file " + sFilePath+" time "+time);
	}
}

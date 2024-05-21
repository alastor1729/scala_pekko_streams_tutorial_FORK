package alpakka.file;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Base64;

/**
 * Java 8 streaming enc/dec roundtrip "Byte by Byte"
 * <p>
 * Avoids OutOfMemoryError:
 * https://stackoverflow.com/questions/9579874/out-of-memory-when-encoding-file-to-base64
 * <p>
 * Same with pekko-streams: alpakka.file.FileIOEcho
 *
 */
public class FileEncDec {
	public static void main(String[] args) {

		String filename = "testfile.jpg";
		Path path = Paths.get("src/main/resources/" + filename);

		try (FileInputStream fis = new FileInputStream(path.toFile())) {
			Base64.Encoder enc1 = Base64.getEncoder();
			Base64.Encoder enc2 = Base64.getMimeEncoder();
			Base64.Encoder enc3 = Base64.getUrlEncoder();
			OutputStream os1 = enc1.wrap(new FileOutputStream(filename + "1.enc"));
			OutputStream os2 = enc2.wrap(new FileOutputStream(filename + "2.enc"));
			OutputStream os3 = enc3.wrap(new FileOutputStream(filename + "3.enc"));
			int _byte;
			while ((_byte = fis.read()) != -1) {
				os1.write(_byte);
				os2.write(_byte);
				os3.write(_byte);
			}
			os1.close();
			os2.close();
			os3.close();
		} catch (IOException ioe) {
			System.err.printf("I/O error: %s%n", ioe.getMessage());
		}
		try (FileOutputStream fos1 = new FileOutputStream("1" + filename);
				FileOutputStream fos2 = new FileOutputStream("2" + filename);
				FileOutputStream fos3 = new FileOutputStream("3" + filename)) {
			Base64.Decoder dec1 = Base64.getDecoder();
			Base64.Decoder dec2 = Base64.getMimeDecoder();
			Base64.Decoder dec3 = Base64.getUrlDecoder();
			InputStream is1 = dec1.wrap(new FileInputStream(filename + "1.enc"));
			InputStream is2 = dec2.wrap(new FileInputStream(filename + "2.enc"));
			InputStream is3 = dec3.wrap(new FileInputStream(filename + "3.enc"));
			int _byte;
			while ((_byte = is1.read()) != -1)
				fos1.write(_byte);
			while ((_byte = is2.read()) != -1)
				fos2.write(_byte);
			while ((_byte = is3.read()) != -1)
				fos3.write(_byte);
			is1.close();
			is2.close();
			is3.close();
		} catch (IOException ioe) {
			System.err.printf("I/O error: %s%n", ioe.getMessage());
		}
	}
}

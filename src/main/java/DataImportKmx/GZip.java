package DataImportKmx;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.Adler32;
import java.util.zip.CheckedInputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class GZip {

	public static final Log LOG = LogFactory.getLog(GZip.class);
	private BufferedOutputStream bufferedOutputStream;
	private String zipFileName = null;

	/**
	 * constructor
	 * 
	 * @param fileName
	 *            : tar file
	 */
	public GZip(String fileName) {
		this.zipFileName = fileName;
	}

	/**
	 * transform zip file to db file
	 * 
	 * @param fileName
	 *            : tar file
	 * @param dir
	 *            : output dir
	 */
	public void zipToDB(String fileName, String dir) {
		try {
			LOG.info("GZip-zipToDB fileName:" + fileName + " directory:" + dir);
			final int BUFFER = 2048;
			BufferedOutputStream dest = null;
			FileInputStream fis = new FileInputStream(fileName);
			CheckedInputStream checkSum = new CheckedInputStream(fis, new Adler32());
			ZipInputStream zis = new ZipInputStream(new BufferedInputStream(checkSum));
			ZipEntry entry;
			while ((entry = zis.getNextEntry()) != null) {
				String tmp = entry.getName();
				String str = dir + "/" + tmp.substring(tmp.lastIndexOf("\\") + 1, tmp.length());
				// System.out.println("Str:"+str);
				int count;
				byte data[] = new byte[BUFFER];
				// write the files to the disk
				FileOutputStream fos = new FileOutputStream(str);
				dest = new BufferedOutputStream(fos, BUFFER);
				while ((count = zis.read(data, 0, BUFFER)) != -1) {
					dest.write(data, 0, count);
				}
				dest.flush();
				dest.close();
			}
			zis.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * UnTar.gz file
	 * 
	 * @param rarFileName
	 *            : input file
	 * @param destDir
	 *            : dest directory
	 * @return
	 * @throws IOException
	 */
	public String unTargzFile(String rarFileName, String destDir) throws IOException {
		LOG.info("GZip-unTargzFile zipFileName:" + zipFileName + " rarFileName:" + rarFileName + " dest Directroy:"
				+ destDir);
		GZip gzip = new GZip(rarFileName);

		String outputDirectory = destDir;

		File file = new File(outputDirectory);
		if (!file.exists()) {
			file.mkdir();
		}
		return gzip.unzipOarFile(outputDirectory);
	}

	/**
	 * uncompress tar file
	 * 
	 * @param outputDirectory
	 *            dest output directory
	 * @return return the outputDirectory+foldername
	 * @throws FileNotFoundException
	 */
	public String unzipOarFile(String outputDirectory) throws FileNotFoundException {
		LOG.info("GZip-unzipOarFile output Directory:" + outputDirectory);
		FileInputStream fis = null;
		ArchiveInputStream in = null;
		String outputPath = null;
		BufferedInputStream bufferedInputStream = null;
		try {
			fis = new FileInputStream(zipFileName);
			GZIPInputStream is = new GZIPInputStream(new BufferedInputStream(fis));
			in = new ArchiveStreamFactory().createArchiveInputStream("tar", is);
			bufferedInputStream = new BufferedInputStream(in);
			TarArchiveEntry entry = (TarArchiveEntry) in.getNextEntry();
			while (entry != null) {
				String name = entry.getName();
				String[] names = name.split("/");
				String fileName = outputDirectory;
				for (int i = 0; i < names.length; i++) {
					String str = names[i];
					fileName = fileName + File.separator + str;
				}
				if (name.endsWith("/")) {
					mkFolder(fileName);
					outputPath = fileName;
				} else {
					File file = mkFile(fileName);
					bufferedOutputStream = new BufferedOutputStream(new FileOutputStream(file));
					int b;
					while ((b = bufferedInputStream.read()) != -1) {
						bufferedOutputStream.write(b);
					}
					bufferedOutputStream.flush();
					bufferedOutputStream.close();
					zipToDB(fileName, outputPath);
					file.delete();
				}
				entry = (TarArchiveEntry) in.getNextEntry();
			}

		} catch (IOException | ArchiveException e) {
			e.printStackTrace();
		} finally {
			try {
				if (bufferedInputStream != null) {
					bufferedInputStream.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return outputPath;
	}

	private void mkFolder(String fileName) {
		File f = new File(fileName);
		if (!f.exists()) {
			f.mkdir();
		}
	}

	private File mkFile(String fileName) {
		File f = new File(fileName);
		try {
			f.createNewFile();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return f;
	}
}
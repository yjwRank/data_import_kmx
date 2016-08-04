package DataImportKmx;

import java.net.URI;
import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

public class DataImportKmx {

	private ImportJob importJob;
	/**
	 * init
	 * 
	 * @param inputDir
	 *            inputPath
	 * @param outputDir
	 *            outputPath
	 * @throws IOException
	 */
	public DataImportKmx(String inputDir, String csv, String outPutDir) throws IOException {
		this.importJob = new ImportJob(inputDir, csv, outPutDir);
	}

	/**
	 * run the impotJob
	 * 
	 * @throws ClassNotFoundException
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void run() throws ClassNotFoundException, IOException, InterruptedException {
		if (this.importJob != null) {
			this.importJob.run();
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		String str1 = args[0];
		String str2 = args[1];
		String str3 = args[2];
		long start = System.currentTimeMillis();
		DataImportKmx test = new DataImportKmx(str1, str2, str3);
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(str3), conf);
		fs.delete(new Path(str3), true);

		test.run();
		long end = System.currentTimeMillis();
		System.out.println("total time:" + (end - start));

	}

}

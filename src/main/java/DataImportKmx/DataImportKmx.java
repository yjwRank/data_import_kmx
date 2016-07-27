package DataImportKmx;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

import java.util.Queue;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.LinkedList;
import java.net.URI;
import java.io.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.sql.SQLException;

public class DataImportKmx {

	private Configuration conf;
	private ImportJob importJob;

	/**
	 * 
	 * init
	 * 
	 * @param inputDir
	 *            inputPath
	 * @param outputDir
	 *            outputPath
	 * @throws IOException
	 */
	public DataImportKmx(String inputDir, String csv, String outPutDir) throws IOException {
		System.out.println("main-dik");
		this.importJob = new ImportJob(inputDir, csv, outPutDir);
	}

	public DataImportKmx() throws IOException {
		System.out.println("main-dik");
	}

	/**
	 * set configuration
	 */
	private void setConf() {
		;
	}
	
	/**
	 * run the impotJob
	 * @throws ClassNotFoundException
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void run() throws ClassNotFoundException, IOException, InterruptedException {
		System.out.println("main-run");
		if (this.importJob != null) {
			this.importJob.run();
		}
	}


	public static void main(String[] args)
			throws IOException, ClassNotFoundException, InterruptedException, SQLException, ParseException {
		System.out.println("main");

		// String str1 = args[0];
		// String str2 = args[1];
		// String str3 = args[2];
		String str1 = "hdfs://localhost:9000/input";
		String str2 = "hdfs://localhost:9000/an/metadata.csv";
		String str3 = "hdfs://localhost:9000/input/tm";
		long start=System.currentTimeMillis();
		DataImportKmx test = new DataImportKmx(str1, str2, str3);
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(str3), conf);
		fs.delete(new Path(str3), true);

		test.run();
		long end=System.currentTimeMillis();
		System.out.println("total:"+(end-start));
	}

}

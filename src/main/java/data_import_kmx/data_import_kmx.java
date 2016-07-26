package data_import_kmx;

import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.TimeZone;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;
import java.net.URI;
import java.io.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.commons.math3.fitting.PolynomialFitter;
import org.apache.commons.math3.fitting.WeightedObservedPoint;

public class data_import_kmx {

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
	public data_import_kmx(String inputDir, String csv, String outputDir) throws IOException {
		System.out.println("main-dik");
		this.importJob = new ImportJob(inputDir, csv, outputDir);
	}

	public data_import_kmx() throws IOException {
		System.out.println("main-dik");
		// this.importJob=new
		// ImportJob("/home/yjw/Desktop/test.tar.gz","/home/yjw/Desktop/output");
	}

	/**
	 * set configuration
	 */
	private void setConf() {
		;
	}

	public void run() throws ClassNotFoundException, IOException, InterruptedException {
		System.out.println("main-run");
		if (this.importJob != null) {
			this.importJob.run();
		}
	}

	public static void unzip(String zipFileName, String outputDirectory) {
		try {
			ZipInputStream in = new ZipInputStream(new FileInputStream(zipFileName));
			ZipEntry z = in.getNextEntry();
			while (z != null) {
				System.out.println("unziping " + z.getName());
				File f = new File(outputDirectory);
				f.mkdir();
				if (z.isDirectory()) {
					String name = z.getName();
					name = name.substring(0, name.length() - 1);
					System.out.println("name " + name);
					f = new File(outputDirectory + File.separator + name);
					f.mkdir();
					System.out.println("mkdir " + outputDirectory + File.separator + name);
				} else {
					f = new File(outputDirectory + File.separator + z.getName());
					f.createNewFile();
					FileOutputStream out = new FileOutputStream(f);
					int b;
					while ((b = in.read()) != -1) {
						out.write(b);
					}
					out.close();
				}
				z = in.getNextEntry();
			}
			in.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static String converToISOTime(String tuibineID) throws ParseException {
		DateFormat turbineID_format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date turbineID_date = turbineID_format.parse(tuibineID);
		TimeZone tz = TimeZone.getTimeZone("UTC");
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
		df.setTimeZone(tz);
		turbineID_format.setLenient(false);
		String ISOTime = df.format(turbineID_date);

		Date now = new Date();
		if (turbineID_date.after(now))
			return null;

		try {
			turbineID_format.parse(tuibineID);
			return ISOTime;
		} catch (Exception e) {
			return null;
		}
		// return ISOTime;
	}

	
	public static void renameFile(String outputPath) throws IOException
	{
		Configuration conf=new Configuration();
		FileSystem fs=FileSystem.get(URI.create(outputPath),conf);
		FileStatus[] status=fs.listStatus(new Path(outputPath));
		Queue<FileStatus> q=new LinkedList<FileStatus>();
		for(FileStatus f:status)
		{
			q.add(f);
		}
		while(q.size()>0)
		{
			FileStatus file=q.poll();
			if(file.isDirectory())
			{
				System.out.println("file:"+file.getPath().toString());
				FileStatus[] status2=fs.listStatus(new Path(file.getPath().toString()));
				for(FileStatus f2:status2)
				{
					System.out.println("f2:"+f2.getPath());
					q.add(f2);
				}
			}
			else
			{
				String name=file.getPath().toString();
				String filename=name;
				System.out.println("filename:"+filename);
				if(filename.contains("/err-2"))
				{
					if(file.getLen()==0)
					{
						System.out.println("000");
					}
				}
				else if(filename.contains("err-r"))
				{
					System.out.println("err-r");
				}
				else if(filename.contains("part"))
				{
					System.out.println("part");
				}
				else if(filename.contains("-r-"))
				{
					System.out.println("-r-");
				}
				else if(filename.contains("SUCCESS"))
				{
					System.out.println("SUCCESS");
				}
			}
			
		}
		
	}
	

	public static void main(String[] args)
			throws IOException, ClassNotFoundException, InterruptedException, SQLException, ParseException {
		System.out.println("main");
	
//		String str1 = args[0];
//		String str2 = args[1];
//		String str3 = args[2];
//		String str1="hdfs://localhost:9000/t";
	 String str1="hdfs://localhost:9000/input";
	String str2="hdfs://localhost:9000/an/metadata.csv";
		 String str3="hdfs://localhost:9000/input/tm";
		

		data_import_kmx test = new data_import_kmx(str1, str2, str3);
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(str3), conf);
		fs.delete(new Path(str3), true);

		test.run();
		
	}

	

}

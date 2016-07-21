package data_import_kmx;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.io.File;
import java.util.Date;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.TimeZone;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.io.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
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

	public static void main(String[] args)
			throws IOException, ClassNotFoundException, InterruptedException, SQLException, ParseException {
		System.out.println("main");
		//String str1 = args[0];
		//String str2 = args[1];
		//String str3 = args[2];
		 String str1="hdfs://localhost:9000/input/testin/GW150001201504.tar.gz";
		 String str2="hdfs://localhost:9000/input/testan";
		 String filename=str1.substring(str1.lastIndexOf('/')+1,str1.length());
		 String str3=str1.substring(0,str1.lastIndexOf('/'))+'/'+filename.substring(0, filename.indexOf('.'))+"-result";
	
	//	 String str3="hdfs://localhost:9000/input/Node1";
		

		data_import_kmx test = new data_import_kmx(str1, str2, str3);
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(str3), conf);
		fs.delete(new Path(str3), true);

		test.run();
		

		// fs.copyFromLocalFile(new Path("/home/yjw/Desktop/test"), new
		// Path("hdfs://localhost:9000/input/Node"));
		// fs.delete(new Path("hdfs://localhost:9000/input/Node/test"),true);

		// System.out.println(args[0]);
		// System.out.println(args[1]);
		// data_import_kmx test=new data_import_kmx(args[0],args[1],args[2]);

		/*
		 * Configuration conf=new Configuration(); FileSystem
		 * fs=FileSystem.get(URI.create("/home/yjw"),conf); fs.mkdirs(new
		 * Path("/home/yjw/Te"));
		 */
		/***
		 * Map => Reduce finished Map Reduce
		 */

		// GZip test=new GZip("hdfs://localhost:9000/input/Node/test.tar");
		// String
		// t=test.unTargzFile("hdfs://localhost:9000/input/Node/test.tar",
		// "hdfs://localhost:9000/input");
		// String str=args[0];
		
		/*  GZip test=new
		  GZip("/home/yjw/Desktop/output1/GW.tar.gz");
		  
		  String
		  t=test.unTargzFile("/home/yjw/Desktop/output1/GW.tar.gz",
		  "/home/yjw/Desktop/output1"); //String
		  t="/home/yjw/Desktop/output1/GW";
		  System.out.println("o:"+t); GoldwindToCSV test2=new GoldwindToCSV();
		  test2.TraversFolder(t);
		  
		  test2.ZipToDB(t); //test2.DbToCSV(t);
		 
		  test2.DbToCSV(t);*/
		/*	String t="/home/yjw/Desktop/input/dsd.csv";
		  analysisCSV test=new analysisCSV(t);
		  test.Run();
		  test.ShowMapDevice();
		  System.out.println("~~~~~~~~~~~~");
		  test.ShowMapSensor();
		  System.out.println("~~~~~~~~~~~~");
		  test.ShowMapResult();*/
	}

	public static void traverseFolder1(String path) {
		int fileNum = 0, folderNum = 0;
		File file = new File(path);
		if (file.exists()) {
			LinkedList<File> list = new LinkedList<File>();
			File[] files = file.listFiles();
			for (File file2 : files) {
				if (file2.isDirectory()) {
					System.out.println("文件夹1:" + file2.getAbsolutePath());
					list.add(file2);
					fileNum++;
				} else {
					System.out.println("文件1:" + file2.getAbsolutePath());
					folderNum++;
				}
			}
			File temp_file;
			while (!list.isEmpty()) {
				temp_file = list.removeFirst();
				files = temp_file.listFiles();
				for (File file2 : files) {
					if (file2.isDirectory()) {
						System.out.println("文件夹2:" + file2.getAbsolutePath());
						list.add(file2);
						fileNum++;
					} else {
						System.out.println("文件2:" + file2.getAbsolutePath());
						folderNum++;
					}
				}
			}
		} else {
			System.out.println("文件不存在!");
		}
		System.out.println("文件夹共有:" + folderNum + ",文件共有:" + fileNum);

	}

}

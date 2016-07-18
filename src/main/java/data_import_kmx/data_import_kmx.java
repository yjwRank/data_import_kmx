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
	 * @param inputDir    inputPath
	 * @param outputDir   outputPath
	 * @throws IOException 
	 */
	public data_import_kmx(String inputDir,String outputDir) throws IOException
	{
		System.out.println("main-dik");
		this.importJob=new ImportJob(inputDir,outputDir);
	}
	
	
	public data_import_kmx() throws IOException
	{
		System.out.println("main-dik");
		this.importJob=new ImportJob("/home/yjw/Desktop/test.tar.gz","/home/yjw/Desktop/output");
	}
	/**
	 * set configuration
	 */
	private void setConf()
	{
		;
	}
	
	
	public void run() throws ClassNotFoundException, IOException, InterruptedException
	{
		System.out.println("main-run");
		if(this.importJob!=null)
		{
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
                }
                else {
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
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
	public static String converToISOTime(String tuibineID) throws ParseException
	  {
		  DateFormat turbineID_format=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		  Date turbineID_date=turbineID_format.parse(tuibineID);
		  TimeZone tz=TimeZone.getTimeZone("UTC");
		  DateFormat df=new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
		  df.setTimeZone(tz);
		  turbineID_format.setLenient(false);
		  String ISOTime=df.format(turbineID_date);
		  
		  Date now=new Date();
		  if(turbineID_date.after(now))
			  return null;
		  
		  
		  try{
			  turbineID_format.parse(tuibineID);
			  return ISOTime;
		  }catch(Exception e)
		  {
			  return null;
		  }
		  //return ISOTime;
	  }
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, SQLException, ParseException
	{
		System.out.println("main");
		data_import_kmx test=new data_import_kmx("/home/yjw/Desktop/test.tar.gz","/home/yjw/Desktop/output");
		test.run();
		
		/***
		 *  Map => Reduce finished Map Reduce 
		 */
		
	/*	GZip test=new GZip("/home/yjw/Desktop/output1/GW150001201504.tar.gz");
		
	String t=test.unTargzFile("/home/yjw/Desktop/output1/GW150001201504.tar.gz", "/home/yjw/Desktop/output1");
		//String t="/home/yjw/Desktop/output1/GW150001201504";
		System.out.println("o:"+t);
		GoldwindToCSV test2=new GoldwindToCSV();
		test2.TraversFolder(t);
		
		test2.ZipToDB(t);*/
	//	test2.DbToCSV(t);
	/*	String filename="/home/yjw/Desktop/output1/GW150001201504/GW15000120150424.db";
        String outputFile="/home/yjw/Desktop/output1/GW150001201504/GW15000120150424.db.csv";
        System.out.println("outputFile:"+outputFile);
        BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile));
        Connection conn = DriverManager.getConnection("jdbc:sqlite:"+filename);
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("select * from RUNDATA");
		ResultSetMetaData rsmd=rs.getMetaData();
		String name=null;
		String TuiName=filename.substring(filename.lastIndexOf('/')+1,filename.lastIndexOf('.'));
		TuiName=TuiName.substring(0, TuiName.length()-8);
		int colNum = rs.getMetaData().getColumnCount();
		for(int i=1;i<=colNum;i++)
		{
			//System.out.println(rsmd.getColumnName(i));
			name=rsmd.getColumnName(i);
			bw.write(name+",");
		}
		bw.write("turbneID"+"\n");
		System.out.println("colNum:"+colNum);
		while(rs.next()) {
			for(int i = 1; i <= colNum; i++) {
				bw.write(rs.getString(i) + ",");
			}
			bw.write(TuiName + "\n");
		}
		conn.close();
		bw.flush();
		bw.close();
	*/
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

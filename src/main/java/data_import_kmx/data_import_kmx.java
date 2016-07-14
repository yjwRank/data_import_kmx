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
	 */
	public data_import_kmx(String inputDir,String outputDir)
	{
		;
	}
	
	
	public data_import_kmx()
	{
		System.out.println("main-dik");
		this.importJob=new ImportJob();
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
           //获取ZipInputStream中的ZipEntry条目，一个zip文件中可能包含多个ZipEntry，
            //当getNextEntry方法的返回值为null，则代表ZipInputStream中没有下一个ZipEntry，
            //输入流读取完成；
            ZipEntry z = in.getNextEntry();
           while (z != null) {
                System.out.println("unziping " + z.getName());
                //创建以zip包文件名为目录名的根目录
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
                //读取下一个ZipEntry
                z = in.getNextEntry();
            }
            in.close();
        }
        catch (Exception e) {
            // TODO 自动生成 catch 块
            e.printStackTrace();
        }
    }
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, SQLException
	{
		/*System.out.println("main");
		data_import_kmx test=new data_import_kmx();
		test.run();
		*/
		/***
		 *  Map => Reduce finished Map Reduce 
		 */
    	/*File f=new File("/home/yjw/Documents/GW15000120150430.db");
    	String string;
    	FileReader fr=new FileReader(f);
    	BufferedReader br=new BufferedReader(fr);
    	while((string=br.readLine())!=null)
    	{
    		System.out.println(string);
    	}*/
   
	/*	Zip test=new Zip();
		test.setZipFileName("/home/yjw/Desktop/output1/GW150001201504/GW15000120150424.zip");
		test.setOutputDirectory("/home/yjw/Desktop/output1/GW150001201504/");
		test.unzip();*/
	//	traverseFolder1("/home/yjw/Desktop/output1/GW150001201504");
		
		/*  File file=new File("/home/yjw/Desktop/output1/GW150001201504/GW15000120150424.goldwind"); //指定文件名及路径
		  GoldwindToCSV test=new GoldwindToCSV();
		  test.GoldWindToZip(file);*/
		
		/*GoldwindToCSV test=new GoldwindToCSV();
		test.TraversFolder("/home/yjw/Desktop/output1/GW150001201504");*/
		
	/*	GZip test=new GZip("/home/yjw/Desktop/output1/GW150001201504.tar.gz");
		
	String t=	test.unTargzFile("/home/yjw/Desktop/output1/GW150001201504.tar.gz", "/home/yjw/Desktop/output1");

		System.out.println("o:"+t);
		GoldwindToCSV test2=new GoldwindToCSV();
		test2.TraversFolder(t);
		
		test2.ZipToDB(t);*/
		String filename="/home/yjw/Desktop/output1/GW150001201504/GW15000120150424.db";
		String outputFile="/home/yjw/Desktop/output1/GW150001201504/GW15000120150424.csv";
BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile));
		
		Class.forName("org.sqlite.JDBC");
		Connection conn = DriverManager.getConnection("jdbc:sqlite:"+filename);
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("select * from RUNDATA");
		int colNum = rs.getMetaData().getColumnCount();
		while(rs.next()) {
			for(int i = 1; i < colNum; i++) {
				bw.write(rs.getString(i) + ",");
			}
			bw.write(rs.getString(colNum) + "\n");
		}
		conn.close();
		bw.flush();
		bw.close();
		conn.close();
		bw.flush();
		bw.close();
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

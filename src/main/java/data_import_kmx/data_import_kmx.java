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
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
	{
	/*	System.out.println("main");
		data_import_kmx test=new data_import_kmx();
		test.run();
		*/
		/***
		 *  Map => Reduce finished Map Reduce 
		 */
		
	/*	analysisCSV test=new analysisCSV("/home/yjw/Desktop/input/metadata.csv");
		analysisCSV test=new analysisCSV("/home/yjw/Desktop/input/dsd.csv");
		test.CSVtoMap();*/
		
	/*	Map<String,Map<String,Integer>> t=new HashMap<String,Map<String,Integer>>();
    	Map<String,Integer> tmm=new HashMap<String,Integer>();
    	tmm.put("WTUR.environment_ok", 1);
    	tmm.put("WMAN.State", 2);
    	t.put("GW25015", tmm);
    	
    	for(Map.Entry<String,Map<String,Integer> > entry:t.entrySet())
		{
    		System.out.println("key: "+entry.getKey()+"  value:"+entry.getValue());
		}
    	
    	System.out.println(t.get("GW25015").get("WMAN.State"));*/
    	File f=new File("/home/yjw/Documents/GW15000120150430.db");
    	String string;
    	FileReader fr=new FileReader(f);
    	BufferedReader br=new BufferedReader(fr);
    	while((string=br.readLine())!=null)
    	{
    		System.out.println(string);
    	}
    	
	}
	
}

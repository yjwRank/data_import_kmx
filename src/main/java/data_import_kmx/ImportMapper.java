package data_import_kmx;

import java.io.IOException;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
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
import java.util.Map;
import java.util.Queue;
import java.util.Vector;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.yarn.webapp.ResponseInfo.Item;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * import job Mapper
 * 
 * @author yjw
 *
 */
public class ImportMapper extends Mapper<Object, Text, Text, LList> {

	private final static IntWritable one = new IntWritable(1);
	private boolean toReduce;
	private Text word = new Text();
	private String tmp = "";
	private LList title = new LList();
	private int tuibineId_loc = -1;
	private int WMAN_Tm = -1;
	private String[] item = null;
	private Map<String, String> device = new HashMap<String, String>();;
	private Map<String, List<String>> sensor = new HashMap<String, List<String>>();
	private Map<String,List<String> > result=new HashMap<String,List<String>>();
	private Map<String, Map<String, Integer>> t = new HashMap<String, Map<String, Integer>>();
	private Queue<String> que = new LinkedList<String>();
	private Queue<String> tmpkey=new LinkedList<String>();
	public static final Log LOG = LogFactory.getLog(ImportReduce.class);
	private String header=null;
	private FSDataOutputStream outputstream;
	private boolean writerr;
	protected void setup(Context context) throws IOException, InterruptedException {
		String fpath = context.getConfiguration().get(FileInputFormat.INPUT_DIR);
		// String path=fpath.substring(fpath.indexOf(':')+1,
		// fpath.lastIndexOf('/'));
		// String path=fpath.substring(0, fpath.lastIndexOf('/'));
		String path = fpath.substring(fpath.indexOf(',') + 1, fpath.length());
		String analysisFile = path;
		writerr=false;
		Configuration conf=new Configuration();
		String output=context.getConfiguration().get(FileOutputFormat.OUTDIR);
		FileSystem fs=FileSystem.get(URI.create(output+"/err-2"), conf);
		outputstream=fs.create(new Path(output+"/err-2"));
		/*
		 * Configuration conf = new Configuration(); FileSystem fs =
		 * FileSystem.get(URI.create(path), conf); FileStatus[]
		 * status=fs.listStatus(new Path(path)); for(FileStatus filet:status) {
		 * String tmp=filet.getPath().toString(); String
		 * tt=tmp.substring(tmp.lastIndexOf('.')+1,tmp.length());
		 * if(tt.equals("csv")) { System.out.println("analys file:"+tmp);
		 * analysisFile=tmp; }
		 * 
		 * }
		 */
		// analysisCSV test=new analysisCSV("/home/yjw/Desktop/input/dsd.csv");

		analysisCSV test = new analysisCSV(analysisFile);
		//test.CSVtoMap();
		test.Run();
		//device = test.getDevice();
		//sensor = test.getSensor();
		result=test.getResult();
		toReduce = false;
		que.clear();
		System.out.println("theeee");
		
		/*for (Map.Entry<String, List<String>> entry : sensor.entrySet()) {
			// System.out.println("Key= "+entry.getKey()+" Value=
			// "+entry.getValue());
			String Key = device.get(entry.getKey());

			int num = 2;// baseIdex

			Map<String, Integer> tmp = new HashMap<String, Integer>();
			for (int i = 0; i < entry.getValue().size(); i++) {

				tmp.put(entry.getValue().get(i), num + i);
			}
			t.put(Key, tmp);

		}*/
		for(Map.Entry<String, List<String>>entry:result.entrySet())
		{
			String Key=entry.getKey();
			int num=2;
			Map<String,Integer> tmp=new HashMap<String,Integer>();
			for(int i=0;i<entry.getValue().size();i++)
			{
				tmp.put(entry.getValue().get(i), num+i);
			}
			t.put(Key, tmp);
		}
	}
	
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		Configuration conf=new Configuration();
		//outputstream.close();
		/*String output=context.getConfiguration().get(FileOutputFormat.OUTDIR);
		FileSystem fs=FileSystem.get(URI.create(output+"/err-2"), conf);
		FileStatus file=fs.getFileStatus(new Path(output+"/err-2"));
		if(file.getLen()==0)
		{
			fs.delete(new Path(output+"/err-2"));
		}*/
		File dictionery=new File("");
		String loc=dictionery.getAbsolutePath()+"/tmp";
		FileUtils.deleteDirectory(new File(loc));
	}

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		System.out.println("mmm");
		
		String filename=value.toString();
		try {
			Connection conn=DriverManager.getConnection("jdbc:sqlite:"+filename);
			Statement stmt=conn.createStatement();
			ResultSet rs=stmt.executeQuery("select * from RUNDATA");
			ResultSetMetaData rsmd=rs.getMetaData();
			String TuiName=null;
			TuiName = filename.substring(filename.lastIndexOf('/') + 1, filename.lastIndexOf('.'));
			TuiName = TuiName.substring(0, TuiName.length() - 8);
			String buffer="";
			int colNum=rs.getMetaData().getColumnCount();
			for(int i=1;i<=colNum;i++)
			{
				buffer+=rsmd.getColumnName(i);
				buffer+=",";
			}
			buffer+="turbineID";
			System.out.println("buffer:"+buffer);
			buffer=buffer.trim();
			if(buffer.charAt(0)=='W')
			{
			WMAN_Tm=-1;
			tuibineId_loc=-1;
			buffer=buffer.replace(".", String.valueOf(""));
			item=buffer.split(",");
			for(int i=0;i<item.length;i++)
			{
				if(item[i].equals("WMANTm"))
				{
					WMAN_Tm=i;
				}
				else if(item[i].equals("turbineID"))
				{
					tuibineId_loc=i;
				}
			}
			}
			else
			{
				LOG.error("don't get title in file:"+filename);
			}
			
			String line="";
			
			if(rs.next())
			{
			for(int i=1;i<=colNum;i++)
			{
				line+=rs.getString(i);
				line+=",";
			}
			line+=TuiName;
			title.clear();
			title.setWMAN_Tm(true);
			String[] item2=line.split(",");
			String Key = item2[tuibineId_loc];
			for(int i=0;i<item.length;i++)
			{
				if(i==tuibineId_loc)
				{
					title.add(0);
				}
				else if(i==WMAN_Tm)
				{
					title.add(1);
				}else
				{
					if(t.get(Key).get(item[i])!=null)
					{
					title.add(t.get(Key).get(item[i]));
					}
				else
				{
					if(writerr==true)
					{
						String message="The file :"+key.toString()+"item "+item[i]+" not find in metadata";
						outputstream.write(message.getBytes());
						writerr=false;
					}
				}
				}
			}
			if(WMAN_Tm==-1)
			{
				
				for(int i=0;i<title.size();i++)
				{
					int num=title.get(i);
					if(num!=0)
					{
						title.Settitle(i, num-1);
					}
				}
				title.setWMAN_Tm(false);
			}
			title.setName(key.toString());
			do
			{
			context.write(new Text(line), title);
			line="";
			for(int i=1;i<=colNum;i++)
			{
				line+=rs.getString(i);
				line+=",";
			}
			line+=TuiName;
			}while(rs.next());
			}
			else
			{
				LOG.error("don't find data in file:"+filename);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	/*	if(line.charAt(0)=='W')
		{
			WMAN_Tm = -1;
			tuibineId_loc = -1;
			line=line.replace(".", String.valueOf(""));
			item=line.split(",");
			//check
			for (int i = 0; i < item.length; i++) {
				if (item[i].equals("WMANTm")) {
					WMAN_Tm = i;
				} else if (item[i].equals("turbineID")) {
					tuibineId_loc = i;
				}
				writerr=true;
			}
		}else
		{
			
			String[] item2 = line.split(",");
			title.clear();
			title.setWMAN_Tm(true);
			String Key = item2[tuibineId_loc];
			System.out.println("kkk:"+Key);
			for (int i = 0; i < item.length; i++) {
				System.out.println("itme:"+item[i]);
				if (i == tuibineId_loc) {
					title.add(0);
				} else if (i == WMAN_Tm) {
					title.add(1);
				} else {
					if(t.get(Key).get(item[i])!=null)
						{
						title.add(t.get(Key).get(item[i]));
						}
					else
					{
						if(writerr==true)
						{
							String message="The file :"+key.toString()+"item "+item[i]+" not find in metadata";
							outputstream.write(message.getBytes());
							writerr=false;
						}
					}
				}
			}
			if(WMAN_Tm==-1)
			{
				
				for(int i=0;i<title.size();i++)
				{
					int num=title.get(i);
					if(num!=0)
					{
						title.Settitle(i, num-1);
					}
				}
				title.setWMAN_Tm(false);
			}
			title.setName(key.toString());
			System.out.println("ooooo");
			context.write(new Text(line), title);*/
		}
		
		
		//System.out.println("tmp:"+tmp+" key:"+key);
		/*if(!key.toString().equals(tmp))
		{
			System.out.println("befor:"+tmp);
			tmp=key.toString();
			System.out.println("after:"+tmp);
			toReduce=false;
			title.clear();
		}
		System.out.println("llllllllllll:"+line);
		if (line.charAt(0) == 'W') {
			System.out.println(line);
			item = line.split(",");
			for (int i = 0; i < item.length; i++) {
				if (item[i].equals("WMAN.Tm")) {
					WMAN_Tm = i;
				} else if (item[i].equals("turbineID")) {
					tuibineId_loc = i;
				}
			}
		} else {
			if (toReduce == false) {
				String[] item2 = line.split(",");

				System.out.println("tuibi:" + tuibineId_loc);
				String Key = item2[tuibineId_loc];

				for (int i = 0; i < item.length; i++) {
					if (i == tuibineId_loc) {
						title.add(0);
					} else if (i == WMAN_Tm) {
						title.add(1);
					} else {
						System.out.println("Key:"+Key+"IIIIIIIIIIIIIIIIIIIIIIIIIIiitem:"+item[i]+"  "+t.get(Key).get(item[i]));
						title.add(t.get(Key).get(item[i]));
					}
				}

				toReduce = true;
				title.setName(key.toString());
				context.write(new Text(line), title);
				System.out.println("`~~~~~~~~~~~~~`:"+title.getList());
				LOG.info("~~~~~~~~~~1:" + line);
			} else {
				title.setName(key.toString());
				context.write(new Text(line), title);
				LOG.info("~~~~~~~2：" + line);
				System.out.println("`~~~~~```:"+title.getList());
			}

		}
		System.out.println("Map end");*/
	}


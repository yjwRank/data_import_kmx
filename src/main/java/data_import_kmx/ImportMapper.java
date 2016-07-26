package data_import_kmx;

import java.io.IOException;
import java.io.FileReader;
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
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
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
public class ImportMapper extends Mapper<Object, Text, Text, Text> {


	private List<Integer> title =null;
	private Map<String,List<String> > result=new HashMap<String,List<String>>();
	private Map<String, Map<String, Integer>> t = new HashMap<String, Map<String, Integer>>();
	private Queue<String> que = new LinkedList<String>();
	private Queue<String> tmpkey=new LinkedList<String>();
	public static final Log LOG = LogFactory.getLog(ImportReduce.class);
	private FSDataOutputStream outputstream;
	private boolean writerr;
	
	
	private void readfile(String filename) throws IOException
	{
		BufferedReader fis=new BufferedReader(new FileReader(filename));
		String line=null;
		while((line=fis.readLine())!=null)
		{
			System.out.println("line:"+line);
		}
	}
	protected void setup(Context context) throws IOException, InterruptedException {
		String fpath = context.getConfiguration().get(FileInputFormat.INPUT_DIR);
		String path = fpath.substring(fpath.indexOf(',') + 1, fpath.length());
		String analysisFile = path;
		writerr=false;
		title=new LinkedList();
		Configuration conf=context.getConfiguration();
		String output=context.getConfiguration().get(FileOutputFormat.OUTDIR);
		FileSystem fs=FileSystem.get(URI.create(output+"/err-2"), conf);
		outputstream=fs.create(new Path(output+"/err-2"));
		URI[] patternsURIs=Job.getInstance(conf).getCacheFiles();
		analysisCSV test=null;
		for(URI patternsURI:patternsURIs)
		{
			Path patternPath=new Path(patternsURI.getPath());
			System.out.println("name:"+patternPath);
			test=new analysisCSV(patternPath.getName().toString());
		}
		System.out.println("log");
		test.Run();
	
		result=test.getResult();

		que.clear();
		System.out.println("theeee");
		
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
		File dictionery=new File("");
		String loc=dictionery.getAbsolutePath()+"/tmp";
		FileUtils.deleteDirectory(new File(loc));
	}
	public String converToISOTime(String tuibineID) throws ParseException {
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
	}
	public void DBwrite(String filename,FileOutputStream out) throws SQLException, ParseException,IOException
	{
		Connection conn=DriverManager.getConnection("jdbc:sqlite:"+filename);
		Statement stmt=conn.createStatement();
		ResultSet rs=stmt.executeQuery("select * from RUNDATA");
		ResultSetMetaData rsmd=rs.getMetaData();
		String TuiName=null;
		TuiName = filename.substring(filename.lastIndexOf('/') + 1, filename.lastIndexOf('.'));
		TuiName = TuiName.substring(0, TuiName.length() - 8);
		String line="";
		int colNum=rs.getMetaData().getColumnCount();
		for(int i=1;i<=colNum;i++)
		{
			line+=rsmd.getColumnName(i);
			line+=",";
		}
		line+="turbineID";
		System.out.println("buffer:"+line);
		line=line.trim();
		int WMAN_Tm=-1;
		int tuibineId_loc=-1;
		String[] item=null;
		if(line.charAt(0)=='W')
		{
			WMAN_Tm=-1;
			tuibineId_loc=-1;
			line=line.replace(".", String.valueOf(""));
			item=line.split(",");
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
			LOG.info("don't get title in file:"+filename);
		}
		line="";
		if(rs.next())
		{
			for(int i=1;i<=colNum;i++)
			{
				line+=rs.getString(i)+",";
			}
			line+=TuiName;
			title.clear();
			String[] item2=line.split(",");
			for(int i=0;i<item.length;i++)
			{
				if(i==tuibineId_loc)
				{
					title.add(0);
				}
				else if(i==WMAN_Tm)
				{
					title.add(1);
				}
				else
				{
					if(t.get(TuiName).get(item[i])!=null)
					{
						title.add(t.get(TuiName).get(item[i]));
					}
					else
					{
						LOG.info("The file :"+filename+"item "+item[i]+" not find in metadata");
					}
				}
			}
			do{
				String buffer="";
				Vector<String> vec=new Vector<String>();
				for(int i=0;i<title.size();i++)
				{
					vec.add("0");
				}
				for(int i=0;i<title.size();i++)
				{
					if(title.get(i)==1)
					{
						String  tmp=converToISOTime(item2[i]);
						if(tmp==null)
						{
							LOG.info("not a date or after Now");
						}
						else{
						vec.setElementAt(converToISOTime(item2[i]), title.get(i));
						}
					}
					else
					{
						vec.setElementAt(item2[i], title.get(i));
					}
				}
				buffer+=vec.get(0);
				for(int i=1;i<vec.size();i++)
				{
					buffer+=",";
					buffer+=vec.get(i);
				}
				buffer+='\n';
				out.write(buffer.getBytes());
				line="";
				for(int i=1;i<=colNum;i++)
				{
					line+=rs.getString(i);
					line+=",";
				}
				line+=TuiName;
				item2=line.split(",");
			}while(rs.next());
		}
	}
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		System.out.println("Key:"+key+"  value:"+value);
		File dictionery=new File("");
		String filename=value.toString();
		String loc=dictionery.getAbsolutePath()+"/tmp"+filename.substring(filename.lastIndexOf('/'), filename.length())
		.replace(".tar.gz", String.valueOf(""))+".csv";
		System.out.println("loc:"+loc);
		File file=new File(value.toString());
		FileOutputStream outputfile=new FileOutputStream(loc);
		if(file.exists())
		{
			Queue<File> list=new LinkedList<File>();
			list.add(file);
			while(list.size()>0)
			{
				File tmp=list.poll();
				if(tmp.isDirectory())
				{
					File[] files=tmp.listFiles();
					for(File file2:files)
					{
						list.add(file2);
					}
				}
				else
				{
					try {
						DBwrite(tmp.getAbsolutePath().toString(),outputfile);
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
			Configuration conf=new Configuration();
			FileSystem fs=FileSystem.get(URI.create(key.toString()),conf);
			fs.copyFromLocalFile(new Path(loc), new Path(key.toString()));
		}
	
	}
	}


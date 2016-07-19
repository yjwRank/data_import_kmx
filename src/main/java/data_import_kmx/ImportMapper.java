package data_import_kmx;

import java.io.IOException;
import java.net.URI;
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

import org.apache.hadoop.conf.Configuration;
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
 * @author yjw
 *
 */
public class ImportMapper extends Mapper<Object,Text,Text,LList>{

	private final static IntWritable one = new IntWritable(1);
	private boolean toReduce;
    private Text word = new Text();
    private String tmp="";
    private LList title=new LList();
    private int tuibineId_loc=-1;
    private int WMAN_Tm=-1;
    private String[] item=null;
    private Map<String,String> device=new HashMap<String,String>();;
    private Map<String,List<String> > sensor=new HashMap<String,List<String> >();
    private Map<String,Map<String,Integer>> t=new HashMap<String,Map<String,Integer>>();
    private Queue<String> que=new LinkedList<String>();
    
    protected void setup(Context context) throws IOException, InterruptedException 
    {
    	String fpath=context.getConfiguration().get(FileInputFormat.INPUT_DIR);
    	//String path=fpath.substring(fpath.indexOf(':')+1, fpath.lastIndexOf('/'));
    	//String path=fpath.substring(0, fpath.lastIndexOf('/'));
    	String path=fpath.substring(fpath.indexOf(',')+1,fpath.length());
    	System.out.println("map-setup:"+path);
    	
    	String analysisFile=path;
    /*	Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(path), conf);
    	FileStatus[] status=fs.listStatus(new Path(path));
    	for(FileStatus filet:status)
    	{
    		String tmp=filet.getPath().toString();
    		String tt=tmp.substring(tmp.lastIndexOf('.')+1,tmp.length());
    		if(tt.equals("csv"))
    		{
    			System.out.println("analys file:"+tmp);
    			analysisFile=tmp;
    		}
    		
    	}*/
    	//analysisCSV test=new analysisCSV("/home/yjw/Desktop/input/dsd.csv");
    	
    	analysisCSV test=new analysisCSV(analysisFile);
    	test.CSVtoMap();
    	device=test.getDevice();
    	sensor=test.getSensor();
    	toReduce=false;
    	que.clear();
    	
    	for(Map.Entry<String, List<String> > entry:sensor.entrySet())
		{
			//System.out.println("Key= "+entry.getKey()+" Value= "+entry.getValue());
    		String Key=device.get(entry.getKey());
    		
    		int num=2;// baseIdex
    		
    		Map<String,Integer> tmp=new HashMap<String,Integer>();
    		for(int i=0;i<entry.getValue().size();i++)
    		{
    			
    			tmp.put(entry.getValue().get(i), num+i);
    		}
    		t.put(Key, tmp);
    		
		}
    	
   /* 	for(Map.Entry<String, Map<String,Integer> > entry:t.entrySet())
		{
    		System.out.println("Key::"+entry.getKey()+"  Value::"+entry.getValue());
		}*/
    }
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
    {
    	  
	      String line=value.toString();
	      StringTokenizer itr = new StringTokenizer(line,",");
	      String token;
	      System.out.println("map-key:"+key+" map-value:"+value);
	      
	/*     while(line.charAt(0)!='W')
	     {
	    	 que.add(line);
	     }
	      item=line.split(",");
	      for(int i=0;i<item.length;i++)
	      {
	    	  if(item[i].equals("WMAN.Tm"))
	    	  {
	    		  WMAN_Tm=i;
	    	  }
	    	  else if(item[i].equals("turbineID"))
	    	  {
	    		  tuibineId_loc=i;
	    	  }
	      }
	      String[] item2=line.split(",");
	      String Key=item2[tuibineId_loc];
	      System.out.println("key:"+Key);
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
				  title.add(t.get(Key).get(item[i]));
			  }
			  System.out.println(title.get(i));
		  }*/
	      
	      if(line.charAt(0)=='W')
	      {
	    	  System.out.println(line);
	    	  item=line.split(",");
	    	  for(int i=0;i<item.length;i++)
	    	  {
	    		  if(item[i].equals("WMAN.Tm"))
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
	    	  if(toReduce==false)
	    	  {
	    		  String[] item2=line.split(",");
	
	    		  System.out.println("tuibi:"+tuibineId_loc);
	    		  String Key=item2[tuibineId_loc];
	  
	    		  
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
	    				  title.add(t.get(Key).get(item[i]));
	    			  }
	    		  }
	    		  
	    		  toReduce=true;
	    		  title.setName(key.toString());
	    		  context.write(new Text(line), title);
	    	  }
	    	  else
	    	  {
	    		  title.setName(key.toString());
	    		  context.write(new Text(line), title);;
	    	  }
	    	
	      }
	      
	      
	    }
}

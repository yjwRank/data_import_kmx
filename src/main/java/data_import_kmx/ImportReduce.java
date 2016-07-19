package data_import_kmx;

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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * import job Reducer
 * @author yjw
 *
 */
public class ImportReduce extends Reducer<Text,LList,NullWritable,Text>{
	private IntWritable result = new IntWritable();
    private  IntWritable own=new IntWritable();
    private MultipleOutputs<NullWritable,Text> mos;
    protected void setup(Reducer<Text,LList,NullWritable,Text>.Context context)
    {
    	mos=new MultipleOutputs<NullWritable,Text>(context);
    }
    
    protected void cleanup(Reducer<Text,LList,NullWritable,Text>.Context context) throws IOException, InterruptedException
    {
    	mos.close();
    }
    
    public String converToISOTime(String tuibineID) throws ParseException
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
    public void reduce(Text key,Iterable<LList> title,Context context
            ) throws IOException, InterruptedException {
    
    for(LList tit:title)
    {
    	String buffer="";
    	String line=key.toString();
    	String[] token1=line.split(",");
    	Vector<String> vec=new Vector<String>();
    	if(token1.length!=tit.size())
    	{
    		System.out.println("error "+token1.length+" "+tit.size()+"  name:"+tit.getname());
    	}
    	else
    	{
    		System.out.println("right:"+token1.length+" name:"+tit.getname());
    		
    		for(int i=0;i<token1.length;i++)
    			vec.add("0");
    		for(int i=0;i<tit.size();i++)
    		{
    			vec.setElementAt(token1[i], tit.get(i));
    		}
    	}
    	if(vec.size()>0)
    	{
    		buffer+=vec.get(0);
    		buffer+=',';
    		try {
				String tmp=converToISOTime(vec.get(1));
				if(tmp==null)
					return;
				buffer+=tmp;
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    		for(int i=1;i<vec.size();i++)
    		{
    			buffer+=",";
    			buffer+=vec.get(i);
    		}
    	}
  //  	mos.write(NullWritable.get(), new Text(buffer), "/home/yjw/Desktop/output/test.csv");	
    	
    	mos.write(NullWritable.get(), new Text(buffer), tit.getname());
    }
   
    }
}

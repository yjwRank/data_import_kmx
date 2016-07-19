package data_import_kmx;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.Q;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;

public class analysisCSV {
		//private BufferedReader reader;
		private FSDataInputStream inputStream;
		private Map<String,String> device;
		private Map<String,List<String> > sensor;
		
		/**
		 * init
		 * @param fileName
		 * @throws IOException 
		 */
		public analysisCSV(String fileName) throws IOException
		{
			//reader=new BufferedReader(new InputStreamReader(new FileInputStream(fileName),"UTF-8"));
			Configuration conf=new Configuration();
			FileSystem fs=FileSystem.get(URI.create(fileName),conf);
			inputStream=fs.open(new Path(fileName));
			device=new HashMap<String,String>();
			sensor=new HashMap<String,List<String> >();
		}
		
		/**
		 * get device
		 * @return
		 */
		public Map<String,String> getDevice()
		{
			return device;
		}
		
		/**
		 * get sensor
		 * @return
		 */
		public Map<String,List<String> > getSensor()
		{
			return sensor;
		}
		
		/**
		 * CSV to Map Map
		 * @return
		 * @throws IOException 
		 */
		public boolean CSVtoMap() throws IOException
		{
			String line=null;
			String mark=null;
			Queue<String> q=new LinkedList<String>();
			while((line=inputStream.readLine())!=null)
			{
				String item[]=line.split(",");
				System.out.println("CSV:"+line);
				if(item[0].contains("<"))
				{
					if(item[0].contains("deviceType"))
					{
						mark="deviceType";
					}
					else if(item[0].contains("sensor"))
					{
						mark="sensor";
						DeviceType(q);
						q.clear();
					}
					else if(item[0].contains("device"))
					{
						mark="device";
						Sensor(q);
						q.clear();
					}
				}
				else
				{
					q.add(line);
				}
			}
			Device(q);
		
			return true;
		}
		
		/**
		 * DeviceType ..
		 * @param q
		 */
		public void DeviceType(Queue<String> q)
		{
			
		}
		
		/**
		 * analysis Sensor => Map<String,List<String>> : <deviceType,id>
		 * @param q
		 */
		public void Sensor(Queue<String> q)
		{
			Map<String,Integer> sensor_map=new HashMap<String,Integer>();
			String line=null;
			line=q.poll();
			String[] item=line.split(",");
			for(int i=0;i<item.length;i++)
			{
				//System.out.println("sensor_map:"+item[i].trim());
				sensor_map.put(item[i].trim(), i);
			}
				
			int num_deviceTypeId=sensor_map.get("deviceTypeId");
			int num_id=sensor_map.get("id");

			while(q.size()>0)
			{
				line=q.poll();
				item=line.split(",");
				if(sensor.get(item[num_deviceTypeId])==null)
				{
					sensor.put(item[num_deviceTypeId],new LinkedList<String>());
				}
				//System.out.println("itemt:"+item[num_deviceTypeId]+" itemID:"+item[num_id].trim());
				sensor.get(item[num_deviceTypeId]).add(item[num_id].trim());
			}
	//		ShowMapSensor();
		}
		
		
		/**
		 * analysis Device => Map<String,String> :<id,deviceTypeId>
		 * @param q
		 */
		public void Device(Queue<String> q)
		{
			Map<String,Integer> Device_map=new HashMap<String,Integer>();
			String line=null;
			line=q.poll();
			String[] item=line.split(",");
			for(int i=0;i<item.length;i++)
			{
				Device_map.put(item[i].trim(), i);
			}
			
			int num_deviceTypeId=Device_map.get("deviceTypeId");
			int num_id=Device_map.get("id");
			
			while(q.size()>0)
			{
				line=q.poll();
				item=line.split(",");
				
				device.put(item[num_deviceTypeId].trim(),item[num_id].trim());
			}
	//		ShowMapDevice();
			
		}
		
		public void ShowMapSensor()
		{
			System.out.println("Sensor Map");
			for(Map.Entry<String, List<String> > entry:sensor.entrySet())
			{
				System.out.println("Key= "+entry.getKey()+" Value= "+entry.getValue());
			}
		}
		
		public void ShowMapDevice()
		{
			System.out.println("Device Map");
			for(Map.Entry<String, String > entry:device.entrySet())
			{
				System.out.println("Key= "+entry.getKey()+" Value= "+entry.getValue());
			}
		}
}

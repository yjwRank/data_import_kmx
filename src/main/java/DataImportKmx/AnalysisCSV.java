package DataImportKmx;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.LinkedList;

public class AnalysisCSV {
	private BufferedReader reader;
	private Map<String, String> device;
	private Map<String, List<String>> sensor;
	private Map<String, List<String>> result;
	private String path;

	/**
	 * constructor
	 * 
	 * @param fileName can be a path contain metadata file or a metadata file
	 * @throws IOException
	 */
	public AnalysisCSV(String fileName) throws IOException {
		reader = new BufferedReader(new FileReader(fileName));
		path = fileName;
		device = new HashMap<String, String>();
		sensor = new HashMap<String, List<String>>();
		result = new HashMap<String, List<String>>();
	}

	/**
	 * 
	 * @return result is analysis result
	 */
	public Map<String, List<String>> getResult() {
		return result;
	}

	/**
	 * get device
	 * 
	 * @return
	 */
	public Map<String, String> getDevice() {
		return device;
	}

	/**
	 * get sensor
	 * 
	 * @return
	 */
	public Map<String, List<String>> getSensor() {
		return sensor;
	}

	/**
	 * read input from inputPath and fine all file
	 * @throws IOException
	 */
	public void run() throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(path), conf);
		// FileStatus[] status=fs.listStatus(new Path(path));
		FileStatus file = fs.getFileStatus(new Path(path));
		Queue<FileStatus> q = new LinkedList<FileStatus>();
		// for(FileStatus file:status)
		// {
		q.add(file);
		// }
		while (q.size() > 0) {
			FileStatus tmp = q.poll();
			if (tmp.isDirectory()) {
				FileStatus[] status2 = fs.listStatus(new Path(tmp.getPath().toString()));
				for (FileStatus file2 : status2) {
					q.add(file2);
				}
			} else {
				csvToMap();
			}
		}
	}

	/**
	 * 
	 * read the metadata file and get "deviceType" block、"sensor" block、"device" block 
	 * @return
	 * @throws IOException
	 */

	public boolean csvToMap() throws IOException {
		String line = null;
		String mark = null;
		Queue<String> q = new LinkedList<String>();
		while ((line = reader.readLine()) != null) {
			String item[] = line.split(",");
			if (item[0].contains("<")) {
				if (item[0].contains("deviceType")) {
					mark = "deviceType";
				} else if (item[0].contains("sensor")) {
					mark = "sensor";
					deviceType(q);
					q.clear();
				} else if (item[0].contains("device")) {
					mark = "device";
					sensor(q);
					q.clear();
				}
			} else {
				q.add(line);
			}
		}
		device(q);
		return true;
	}

	/**
	 * DeviceType ..none
	 * 
	 * @param q
	 */
	public void deviceType(Queue<String> q) {

	}

	/**
	 * analysis Sensor => Map< String,List< String > > : < deviceType,id >
	 * 
	 * @param q
	 */
	public void sensor(Queue<String> q) {
		Map<String, Integer> sensorMap = new HashMap<String, Integer>();
		String line = null;
		line = q.poll();
		String[] item = line.split(",");
		for (int i = 0; i < item.length; i++) {
			sensorMap.put(item[i].trim(), i);
		}

		int numDeviceTypeId = sensorMap.get("deviceTypeId");
		int numId = sensorMap.get("id");

		while (q.size() > 0) {
			line = q.poll();
			item = line.split(",");
			if (sensor.get(item[numDeviceTypeId]) == null) {
				sensor.put(item[numDeviceTypeId], new LinkedList<String>());
			}
			sensor.get(item[numDeviceTypeId]).add(item[numId].trim());
		}
	}

	/**
	 * analysis Device => Map< String,String > : < id,deviceTypeId >
	 * 
	 * @param q
	 */
	public void device(Queue<String> q) {
		Map<String, Integer> deviceMap = new HashMap<String, Integer>();
		String line = null;
		line = q.poll();
		String[] item = line.split(",");
		for (int i = 0; i < item.length; i++) {
			deviceMap.put(item[i].trim(), i);
		}

		int numDeviceTypeId = deviceMap.get("deviceTypeId");
		int numId = deviceMap.get("id");
		while (q.size() > 0) {
			line = q.poll();
			item = line.split(",");
			device.put(item[numDeviceTypeId].trim(), item[numId].trim());
			result.put(item[numId].trim(), sensor.get(item[numDeviceTypeId].trim()));
		}
	}

	public void showMapSensor() {
		System.out.println("Sensor Map");
		for (Map.Entry<String, List<String>> entry : sensor.entrySet()) {
			System.out.println("Key= " + entry.getKey() + " Value= " + entry.getValue());
		}
	}

	public void showMapDevice() {
		System.out.println("Device Map");
		for (Map.Entry<String, String> entry : device.entrySet()) {
			System.out.println("Key= " + entry.getKey() + " Value= " + entry.getValue());
		}
	}

	public void showMapResult() {
		System.out.println("Result Map");
		for (Map.Entry<String, List<String>> entry : result.entrySet()) {
			System.out.println("Key= " + entry.getKey() + " Value= " + entry.getValue());
		}
	}
}

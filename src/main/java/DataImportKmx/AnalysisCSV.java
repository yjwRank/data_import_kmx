package DataImportKmx;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.LinkedList;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

public class AnalysisCSV {
	private BufferedReader reader;
	private Map<String, String> device;
	private Map<String, List<String>> sensor;
	private Map<String, List<String>> result;
	private String path;
	public static final Log LOG = LogFactory.getLog(AnalysisCSV.class);

	public AnalysisCSV() {
		path = null;
		device = new HashMap<String, String>();
		sensor = new HashMap<String, List<String>>();
		result = new HashMap<String, List<String>>();
	}

	/**
	 * constructor
	 * 
	 * @param fileName
	 *            can be a path contain metadata file or a metadata file
	 * @throws IOException
	 */
	public AnalysisCSV(String fileName) throws IOException {
		LOG.info("analysis constructor filename:" + fileName);
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

	public void add(String fileName) throws IOException {
		LOG.info("AnalysisCSV-add fileName:" + fileName);
		reader = new BufferedReader(new FileReader(fileName));
		path = fileName;
		run();
	}

	/**
	 * read input from inputPath and fine all file
	 * 
	 * @throws IOException
	 */
	public void run() throws IOException {
		LOG.info("AnalysisCSV-run");
		File file = new File(path);
		Queue<File> q = new LinkedList<File>();
		q.add(file);
		while (q.size() > 0) {
			File tmp = q.poll();
			if (tmp.isDirectory()) {
				File[] status2 = tmp.listFiles();
				for (File file2 : status2) {
					q.add(file2);
				}
			} else {
				turnCsvToMap();
			}
		}
	}

	/**
	 * 
	 * read the metadata file and get "deviceType" block、"sensor" block、"device"
	 * block
	 * 
	 * @return
	 * @throws IOException
	 */

	public boolean turnCsvToMap() throws IOException {
		String line = null;
		LOG.info("Analysis-turnCsvToMap");
		Queue<String> q = new LinkedList<String>();
		while ((line = reader.readLine()) != null) {
			String item[] = line.split(",");
			if (item[0].contains("<")) {
				if (item[0].contains("deviceType")) {
					;
				} else if (item[0].contains("sensor")) {
					getDeviceType(q);
					q.clear();
				} else if (item[0].contains("device")) {
					getSensor(q);
					q.clear();
				}
			} else {
				q.add(line);
			}
		}
		getDevice(q);
		return true;
	}

	/**
	 * DeviceType ..none
	 * 
	 * @param q
	 */
	public void getDeviceType(Queue<String> q) {

	}

	/**
	 * analysis Sensor => Map< String,List< String > > : < deviceType,id >
	 * 
	 * @param q
	 */
	public void getSensor(Queue<String> q) {
		LOG.info("AnalysisCSV-getSensor");
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
	public void getDevice(Queue<String> q) {
		LOG.info("AnalysisCSV-getDevice");
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

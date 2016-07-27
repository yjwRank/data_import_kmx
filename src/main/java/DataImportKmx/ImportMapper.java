package DataImportKmx;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;


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
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.util.Date;
import java.util.HashMap;
import java.util.TimeZone;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Vector;

/**
 * import job Mapper
 * 
 * @author yjw
 *
 */
public class ImportMapper extends Mapper<Object, Text, Text, Text> {

	private List<Integer> title = null;
	private Map<String, List<String>> result = new HashMap<String, List<String>>();
	private Map<String, Map<String, Integer>> t = new HashMap<String, Map<String, Integer>>();
	private Queue<String> que = new LinkedList<String>();
	public static final Log LOG = LogFactory.getLog(ImportReduce.class);

	private void readfile(String filename) throws FileNotFoundException
	{
		BufferedReader fis=new BufferedReader(new FileReader(filename));
		String line;
		try {
			while((line=fis.readLine())!=null)
			{
				System.out.println("line:"+line);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	protected void setup(Context context) throws IOException, InterruptedException {
		title = new LinkedList();
		Configuration conf = context.getConfiguration();
		URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
		AnalysisCSV test = null;
		for (URI patternsURI : patternsURIs) {
			Path patternPath = new Path(patternsURI.getPath());
			System.out.println("name:" + patternPath);
			patternPath.toString();
			test = new AnalysisCSV(patternPath.getName().toString());
		}
		test.run();

		result = test.getResult();
		que.clear();

		for (Map.Entry<String, List<String>> entry : result.entrySet()) {
			String key = entry.getKey();
			int num = 2;
			Map<String, Integer> tmp = new HashMap<String, Integer>();
			for (int i = 0; i < entry.getValue().size(); i++) {
				tmp.put(entry.getValue().get(i), num + i);
			}
			t.put(key, tmp);
		}
	}

	protected void cleanup(Context context) throws IOException, InterruptedException {
		File dictionery = new File("");
		String loc = dictionery.getAbsolutePath() + "/tmp";
		FileUtils.deleteDirectory(new File(loc));
	}

	public String converToISOTime(String tuibineID) throws ParseException {
		DateFormat turbineIdFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date turbineIdDate = turbineIdFormat.parse(tuibineID);
		TimeZone tz = TimeZone.getTimeZone("UTC");
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
		df.setTimeZone(tz);
		turbineIdFormat.setLenient(false);
		String ISOTime = df.format(turbineIdDate);

		Date now = new Date();
		if (turbineIdDate.after(now))
			return null;

		try {
			turbineIdFormat.parse(tuibineID);
			return ISOTime;
		} catch (Exception e) {
			return null;
		}
	}

	public void dbWrite(String filename,FSDataOutputStream out) throws SQLException, ParseException, IOException {
		Connection conn = DriverManager.getConnection("jdbc:sqlite:" + filename);
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("select * from RUNDATA");
		ResultSetMetaData rsmd = rs.getMetaData();
		String tuiName = null;
		tuiName = filename.substring(filename.lastIndexOf('/') + 1, filename.lastIndexOf('.'));
		tuiName = tuiName.substring(0, tuiName.length() - 8);
		String line = "";
		int colNum = rs.getMetaData().getColumnCount();
		for (int i = 1; i <= colNum; i++) {
			line += rsmd.getColumnName(i);
			line += ",";
		}
		line += "turbineID";
		line = line.trim();
		int wMANTm = -1;
		int tuibineIdLoc = -1;
		String[] item = null;
		if (line.charAt(0) == 'W') {
			wMANTm = -1;
			tuibineIdLoc = -1;
			line = line.replace(".", String.valueOf(""));
			item = line.split(",");
			for (int i = 0; i < item.length; i++) {
				if (item[i].equals("WMANTm")) {
					wMANTm = i;
				} else if (item[i].equals("turbineID")) {
					tuibineIdLoc = i;
				}
			}
		} else {
			LOG.info("don't get title in file:" + filename);
		}
		line = "";
		if (rs.next()) {
			for (int i = 1; i <= colNum; i++) {
				line += rs.getString(i) + ",";
			}
			line += tuiName;
			title.clear();
			String[] item2 = line.split(",");
			for (int i = 0; i < item.length; i++) {
				if (i == tuibineIdLoc) {
					title.add(0);
				} else if (i == wMANTm) {
					title.add(1);
				} else {
					if (t.get(tuiName).get(item[i]) != null) {
						title.add(t.get(tuiName).get(item[i]));
					} else {
						LOG.info("The file :" + filename + "item " + item[i] + " not find in metadata");
					}
				}
			}
			do {
				String buffer = "";
				Vector<String> vec = new Vector<String>();
				for (int i = 0; i < title.size(); i++) {
					vec.add("0");
				}
				for (int i = 0; i < title.size(); i++) {
					if (title.get(i) == 1) {
						String tmp = converToISOTime(item2[i]);
						if (tmp == null) {
							LOG.info("not a date or after Now");
						} else {
							vec.setElementAt(converToISOTime(item2[i]), title.get(i));
						}
					} else {
						vec.setElementAt(item2[i], title.get(i));
					}
				}
				buffer += vec.get(0);
				for (int i = 1; i < vec.size(); i++) {
					buffer += ",";
					buffer += vec.get(i);
				}
				buffer += '\n';
				out.write(buffer.getBytes());
				line = "";
				for (int i = 1; i <= colNum; i++) {
					line += rs.getString(i);
					line += ",";
				}
				line += tuiName;
				item2 = line.split(",");
			} while (rs.next());
		}
	}

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		File dictionery = new File("");
		String filename = value.toString();
		String turnname=filename
				.substring(filename.lastIndexOf('/'), filename.length()).replace(".tar.gz", String.valueOf(""))
				+ ".csv";
		String loc = dictionery.getAbsolutePath() + "/tmp" + turnname;
		File file = new File(value.toString());
		//FileOutputStream outputfile = new FileOutputStream(loc);
		System.out.println("dbwrite"+filename+" "+turnname+" "+key.toString());
		Configuration conf = new Configuration();
		
		FileSystem fs1 = FileSystem.get(URI.create(key.toString()+turnname), conf);
		FSDataOutputStream outputstream=fs1.create(new Path(key.toString()+turnname));
		
		if (file.exists()) {
			Queue<File> list = new LinkedList<File>();
			list.add(file);
			while (list.size() > 0) {
				File tmp = list.poll();
				if (tmp.isDirectory()) {
					File[] files = tmp.listFiles();
					for (File file2 : files) {
						list.add(file2);
					}
				} else {
					try {
					
						dbWrite(tmp.getAbsolutePath().toString(), outputstream);
					} catch (SQLException | ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
			//Configuration conf = new Configuration();
			//FileSystem fs = FileSystem.get(URI.create(key.toString()), conf);
			//fs.copyFromLocalFile(new Path(loc), new Path(key.toString()));
		}

	}
}

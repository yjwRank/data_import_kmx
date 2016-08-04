package DataImportKmx;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * import job Mapper
 * 
 * @author yjw
 *
 */
public class ImportMapper extends Mapper<Object, Text, Text, Text> {

	private Map<String, List<String>> result = new HashMap<String, List<String>>();
	private Queue<String> que = new LinkedList<String>();
	private Properties props = new Properties();
	public static final Log LOG = LogFactory.getLog(ImportMapper.class);

	protected void setup(Context context) throws IOException, InterruptedException {
		LOG.info("ImportMapper-setup");
		Configuration conf = context.getConfiguration();
		URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
		AnalysisCSV test = new AnalysisCSV();
		for (URI patternsURI : patternsURIs) {
			Path patternPath = new Path(patternsURI.getPath());
			LOG.info("ImportMapper-setup patternPath:" + patternPath);
			if (patternPath.getName().toString().contains(".csv")) {
				LOG.info("ImportMapper-setup add metadata file :" + patternPath.getName());
				test.add(patternPath.getName().toString());
			} else {
				LOG.info("ImportMapper-setup add Properties file :" + patternPath.getName());
				InputStream in = new BufferedInputStream(new FileInputStream(patternPath.getName().toString()));
				props.load(in);
			}
		}

		result = test.getResult();
		que.clear();

	}

	protected void cleanup(Context context) throws IOException, InterruptedException {
		File dictionery = new File("");
		String loc = dictionery.getAbsolutePath() + "/tmp";
		FileUtils.deleteDirectory(new File(loc));
		LOG.info("ImportMapper-cleanup  directory:" + loc);
	}

	/**
	 * select the result from db file and write the result to csv file
	 * 
	 * @param filename
	 *            : the db filename
	 * @param outputStream
	 *            : outputStream to csv file
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 * @throws IOException
	 */
	public void writeResultFromDb(String filename, FileOutputStream outputStream)
			throws ClassNotFoundException, SQLException, IOException {
		LOG.info("ImportMapper-writeResultFromDb filename:" + filename);
		Class.forName("org.sqlite.JDBC");
		Connection conn = DriverManager.getConnection("jdbc:sqlite:" + filename);
		Statement stmt = conn.createStatement();
		StringBuffer sql = new StringBuffer();
		String key = filename.substring(filename.lastIndexOf('/') + 1, filename.length() - 11);
		sql.append("select ");
		sql.append("strftime('%Y-%m-%dT%H:%M:%SZ',\"WMAN.Tm\") as WMANTm" + ",");
		sql.append("\"" + props.getProperty(result.get(key).get(0)) + "\"");
		for (int i = 1; i < result.get(key).size(); i++) {
			sql.append("," + "\"" + props.getProperty(result.get(key).get(i)) + "\"");
		}
		sql.append(" from RUNDATA");
		ResultSet rs = stmt.executeQuery(sql.toString());
		int colNum = rs.getMetaData().getColumnCount();
		StringBuffer buffer = new StringBuffer();
		while (rs.next()) {
			buffer.delete(0, buffer.length());
			buffer.append(key);

			for (int i = 1; i <= colNum; i++) {
				buffer.append("," + rs.getString(i));
			}
			buffer.append('\n');

			outputStream.write(buffer.toString().getBytes());
		}
	}

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		LOG.info("ImportMapper-map");
		File dictionery = new File("");
		boolean putErrorCsv = false;
		String filename = value.toString();
		String turnname = filename.substring(filename.lastIndexOf('/'), filename.length()).replace(".tar.gz",
				String.valueOf("")) + ".csv";
		String loc = dictionery.getAbsolutePath() + "/tmp" + turnname;
		String err = loc.replace(".csv", "-err.csv");
		File file = new File(value.toString());
		System.out.println("dbwrite" + filename + " " + turnname + " " + key.toString());
		LOG.info("ImportMapper-map  filename:" + filename + "  turnname:" + turnname + " outputfilename:"
				+ key.toString());
		FileOutputStream outputstream = new FileOutputStream(loc);

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
						writeResultFromDb(tmp.getAbsolutePath().toString(), outputstream);
						LOG.info("ImportMapper-map db file:" + tmp.getAbsolutePath().toString());
					} catch (SQLException | ClassNotFoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}

			}

			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(URI.create(key.toString()), conf);
			fs.copyFromLocalFile(new Path(loc), new Path(key.toString()));
			if (putErrorCsv) {
				fs.copyFromLocalFile(new Path(err), new Path(key.toString()));
			}

		}

	}
}

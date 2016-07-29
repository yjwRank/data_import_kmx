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
import java.io.InputStream;
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
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.util.Date;
import java.util.HashMap;
import java.util.TimeZone;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;

/**
 * import job Mapper
 * 
 * @author yjw
 *
 */
public class ImportMapper extends Mapper<Object, Text, Text, Text> {

	private Map<String, List<String>> result = new HashMap<String, List<String>>();
	private Queue<String> que = new LinkedList<String>();
	private Properties props=new Properties();
	public static final Log LOG = LogFactory.getLog(ImportReduce.class);

	private void readfile(String filename) throws IOException
	{	
		BufferedReader fis=new BufferedReader(new FileReader(filename));
		String line;
		try {
			while((line=fis.readLine())!=null)
			{
				System.out.println("line:"+line);
				LOG.info("line:"+line);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
		AnalysisCSV test = new AnalysisCSV();
		for (URI patternsURI : patternsURIs) {
			Path patternPath = new Path(patternsURI.getPath());
			LOG.info("~~~~~~~~~~~~~~~~~~~~patternPath:"+patternPath);
			if(patternPath.getName().toString().contains(".csv"))
			{
				test.add(patternPath.getName().toString());
			}
			else
			{
				InputStream in= new BufferedInputStream (new FileInputStream(patternPath.getName().toString()));
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
	}

	

	public void dBWrite(String filename,FileOutputStream output) throws ClassNotFoundException, SQLException, IOException
	{
		Class.forName("org.sqlite.JDBC");
		Connection conn = DriverManager.getConnection("jdbc:sqlite:"+filename);
		Statement stmt = conn.createStatement();
		StringBuffer sql=new StringBuffer();
		String key=filename.substring(filename.lastIndexOf('/')+1,filename.length()-11);
		sql.append("select ");
		sql.append("strftime('%Y-%m-%dT%H:%M:%SZ',\"WMAN.Tm\") as WMANTm"+",");
		sql.append("\""+props.getProperty(result.get(key).get(0))+"\"");
		for(int i=1;i<result.get(key).size();i++)
		{
			sql.append(","+"\""+props.getProperty(result.get(key).get(i))+"\"");
		}
		sql.append(" from RUNDATA");
		ResultSet rs = stmt.executeQuery(sql.toString());
		ResultSetMetaData rsmd = rs.getMetaData();
		int colNum=rs.getMetaData().getColumnCount();
		StringBuffer buffer=new StringBuffer();
		while(rs.next())
		{
			buffer.delete(0, buffer.length());
			buffer.append(key);
			for(int i=1;i<=colNum;i++)
			{
				buffer.append(","+rs.getString(i));
			}
			buffer.append('\n');
			output.write(buffer.toString().getBytes());
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
		System.out.println("dbwrite"+filename+" "+turnname+" "+key.toString());
/*		Configuration conf = new Configuration();
		
		FileSystem fs1 = FileSystem.get(URI.create(key.toString()+turnname), conf);
		FSDataOutputStream outputstream=fs1.create(new Path(key.toString()+turnname));*/
		FileOutputStream outputstream=new FileOutputStream(loc);
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
						
							dBWrite(tmp.getAbsolutePath().toString(), outputstream);
							
					} catch (SQLException | ClassNotFoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
					
			}
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(URI.create(key.toString()), conf);
			fs.copyFromLocalFile(new Path(loc), new Path(key.toString()));
			
		}

	}
}

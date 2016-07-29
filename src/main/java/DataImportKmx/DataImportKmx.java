package DataImportKmx;

import org.apache.commons.math3.util.MultidimensionalCounter.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;

import java.util.Queue;
import java.util.Set;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.TimeZone;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.net.URI;
import java.io.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class DataImportKmx {

	private Configuration conf;
	private ImportJob importJob;

	/**
	 * 
	 * init
	 * 
	 * @param inputDir
	 *            inputPath
	 * @param outputDir
	 *            outputPath
	 * @throws IOException
	 */
	public DataImportKmx(String inputDir, String csv, String outPutDir) throws IOException {
		System.out.println("main-dik");
		this.importJob = new ImportJob(inputDir, csv, outPutDir);
	}

	public DataImportKmx() throws IOException {
		System.out.println("main-dik");
	}

	/**
	 * set configuration
	 */
	private void setConf() {
		;
	}
	
	/**
	 * run the impotJob
	 * @throws ClassNotFoundException
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void run() throws ClassNotFoundException, IOException, InterruptedException {
		System.out.println("main-run");
		if (this.importJob != null) {
			this.importJob.run();
		}
	}

	
	public static void main(String[] args)
			throws IOException, ClassNotFoundException, InterruptedException, SQLException, ParseException {
		System.out.println("main");
		

		 String str1 = args[0];
		 String str2 = args[1];
		 String str3 = args[2];
//		String str1 = "hdfs://localhost:9000/input";
//		String str2 = "hdfs://localhost:9000/an";
//		String str3 = "hdfs://localhost:9000/input/tm";
		long start=System.currentTimeMillis();
		DataImportKmx test = new DataImportKmx(str1, str2, str3);
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(str3), conf);
		fs.delete(new Path(str3), true);
		
		test.run();
		long end=System.currentTimeMillis();
		System.out.println("total:"+(end-start));
	
	
/*		String metadata="/home/yjw/Desktop/input/metadata1.csv";
		AnalysisCSV an=new AnalysisCSV(metadata);
		an.csvToMap();		
		String fileName="/home/yjw/Desktop/GW15000120150425.db";
		String key=fileName.substring(fileName.lastIndexOf('/')+1,fileName.length()-11);
		Map<String,List<String>> result=new HashMap<String,List<String>>();
		result=an.getResult();
		List<String> list=result.get(key);
		String sql_converTime="strftime('%Y-%m-%dT%H:%M:%SZ',\"WMAN.Tm\") as WMANTm";
		Class.forName("org.sqlite.JDBC");
		Connection conn = DriverManager.getConnection("jdbc:sqlite:"+fileName);
		Statement stmt = conn.createStatement();
		StringBuffer sql=new StringBuffer();
		sql.append("select ");
		sql.append("\""+list.get(0)+"\"");
		for(int i=1;i<list.size();i++)
		{
			sql.append(",");
			sql.append("\""+list.get(i)+"\"");
		}
		sql.append(","+sql_converTime);
		sql.append(" from RUNDATA");
		System.out.println("ss:"+sql.toString());
		ResultSet rs = stmt.executeQuery(sql.toString());
		ResultSetMetaData rsmd = rs.getMetaData();
		FileOutputStream outputfile=new FileOutputStream("/home/yjw/Desktop/ddddd.csv");
		int colNum=rs.getMetaData().getColumnCount();
		StringBuffer buffer=new StringBuffer();
		for(int i=1;i<=colNum;i++)
		{
			buffer.append(rsmd.getColumnName(i)+",");
		}
		buffer.append("turbineID"+"\n");
		outputfile.write(buffer.toString().getBytes());
		while(rs.next())
		{
			buffer.delete(0, buffer.length());
			for(int i=1;i<=colNum;i++)
			{
				
				buffer.append(rs.getString(i)+",");
				
			}
			buffer.append(key+"\n");
			outputfile.write(buffer.toString().getBytes());
		}
		outputfile.close();
		*/
	}

}

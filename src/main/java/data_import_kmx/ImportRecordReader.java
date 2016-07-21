package data_import_kmx;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URL;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.zip.GZIPInputStream;
import java.util.Queue;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ImportRecordReader extends RecordReader<Text, Text> {

	private Text key;
	private Text value;
	private String file;
	private List<File> filelist;
	private Queue<String> keyvalue;
	private String outpath;
	public static final Log LOG = LogFactory.getLog(ImportRecordReader.class);

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		file = context.getConfiguration().get(FileInputFormat.INPUT_DIR);
		//System.out.println("importRecorde:" + file);
		outpath = context.getConfiguration().get(FileOutputFormat.OUTDIR);
		String file1 = file.substring(0, file.lastIndexOf(','));
		//System.out.println("file1：" + file1);
		Configuration conf = new Configuration();
		File directory = new File("");
		
		FileSystem fs = FileSystem.get(URI.create(file1.toString()), conf);
		//FileSystem localfs = FileSystem.get(URI.create(directory.getAbsolutePath()), conf);
		String loc = directory.getAbsolutePath() + "/tmp";
		File dir=new File(loc);
		//localfs.mkdirs(new Path(loc));
		dir.mkdir();
		String fpath = file1.toString().trim();
		String filename = fpath.substring(fpath.lastIndexOf('/'), fpath.length());
		String locfile = loc + filename;
		System.out.println("locfile:" + locfile);
		fs.copyToLocalFile(new Path(file1), new Path(locfile));
		keyvalue = new LinkedList<String>();
		String path = locfile.substring(0, locfile.lastIndexOf('/'));
	
		GZip turn = new GZip(locfile);
		String tfile = turn.unTargzFile(locfile, path);
		//System.out.println("tfile:" + tfile);
		//System.out.println("path:" + path);

		GoldwindToCSV turn2 = new GoldwindToCSV();
		turn2.TraversFolder(tfile);

		turn2.ZipToDB(tfile);
		try {
			turn2.DbToCSV(tfile);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}

		String hdfsoutputpath = fpath.substring(0, fpath.lastIndexOf('/'));
		//System.out.println("hdfsoutputpath:" + hdfsoutputpath);
		String fullpath = hdfsoutputpath + tfile.substring(tfile.lastIndexOf('/'), tfile.length());
		//System.out.println(fullpath);
		if (fs.exists(new Path(fullpath))) {
			fs.delete(new Path(fullpath), true);
		} else {
			System.out.println("not exit");
		}
		//fs.copyFromLocalFile(new Path(tfile), new Path(hdfsoutputpath));

		//System.out.println("finish");
		//getFileInfo(fullpath);
		LOG.info("~~~~~~~~~~~~~~~~getInfo");
		LOG.info("loc"+loc+tfile.substring(tfile.lastIndexOf('/'), tfile.length()));
		getFileInfo1(loc+tfile.substring(tfile.lastIndexOf('/'), tfile.length()));
		
		FileUtils.deleteDirectory(new File(loc));
		//localfs.delete(new Path(loc));
		//fs.delete(new Path(fullpath));
	}

	/*
	 * private void mkFolder(String fileName) { File f = new File(fileName); if
	 * (!f.exists()) { f.mkdir(); } }
	 * 
	 * private File mkFile(String fileName) { File f = new File(fileName); try {
	 * f.createNewFile(); } catch (IOException e) { e.printStackTrace(); }
	 * return f; }
	 */
	public void getFileInfo(String path) throws IOException {
		int fileNum = 0, folderNum = 0;
		LOG.info("getFileInfor  path:" + path);
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(path), conf);
		FileStatus[] status = fs.listStatus(new Path(path));
		Queue<FileStatus> q = new LinkedList<FileStatus>();
		for (FileStatus file : status) {
			q.add(file);
		}

		while (q.size() > 0) {
			FileStatus tmp = q.poll();
			if (tmp.isDirectory()) {
				System.out.println("文件夹：" + tmp.getPath());
				FileStatus[] status2 = fs.listStatus(new Path(tmp.getPath().toString()));
				for (FileStatus file2 : status2) {
					q.add(file2);
				}
			} else {
				LOG.info("             wenjian :"+tmp.getPath());
				System.out.println("文件：" + tmp.getPath());
				turntomap(tmp.getPath().toString());
			}
		}

	}
	public void getFileInfo1(String path) throws IOException
	{
		File file=new File(path);
		if(file.exists())
		{
			Queue<File> list=new LinkedList<File>();
			File[] files=file.listFiles();
			for(File file2:files)
			{
				list.add(file2);
			}
			while(list.size()>0)
			{
				File tmp=list.poll();
				if(tmp.isDirectory())
				{
					File[] files2=tmp.listFiles();
					for(File file3:files2)
					{
						list.add(file3);
					}
				}
				else
				{
					turntomap1(tmp.getAbsolutePath().toString());
				}
			}
			
			
		}
	}
	public void turntomap(String file) throws IOException {

		// BufferedReader reader =new BufferedReader(new InputStreamReader(new
		// FileInputStream(file),"UTF-8"));
		String name1 = file.toString();
		String name = outpath + name1.substring(name1.lastIndexOf('/'), name1.length());
		String line = null;
		// Map<String,String> tmp=new HashMap<String,String>();
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(file), conf);
		FSDataInputStream inputStream = fs.open(new Path(file));
		while ((line = inputStream.readLine()) != null) {

			keyvalue.add(name + "$" + line);
			// System.out.println("line+:"+line);
		}
	}
	
	public void turntomap1(String file) throws IOException{
		String name1=file;
		String name = outpath + name1.substring(name1.lastIndexOf('/'), name1.length());
		String line = null;
		 BufferedReader reader =  new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF-8"));
		 while((line=reader.readLine())!=null)
		 {
			 keyvalue.add(name+"$"+line);
		 }
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String k = null;
		String v = null;
		boolean lock = false;
		if (keyvalue.size() > 0) {
			String line = keyvalue.poll();
			k = line.substring(0, line.indexOf('$'));
			if (line.indexOf('$') + 1 < line.length())
				v = line.substring(line.indexOf('$') + 1, line.length());
			key = new Text(k);
			value = new Text(v);
			lock = true;
		}

		return lock;
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub

	}

}

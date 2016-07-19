package data_import_kmx;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
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


public class ImportRecordReader extends RecordReader<Text, Text>{

	private Text key;
	private Text value;
	private String file;
	private List<File> filelist;
	private Queue<String> keyvalue;
	public static final Log LOG = LogFactory.getLog(ImportRecordReader.class);
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
	/*	System.out.println("gergergergergr:"+context.getConfiguration().get(FileInputFormat.INPUT_DIR));
		keyvalue=new LinkedList<String>();
		file=context.getConfiguration().get(FileInputFormat.INPUT_DIR);
		String outputpath=file.substring(file.indexOf(':')+1,file.length());
		System.out.println("outputPath:"+outputpath);
		String path=outputpath.substring(0, outputpath.lastIndexOf('/'));
		System.out.println("path"+path);
		GZip turn=new GZip(outputpath);
		String tfile=turn.unTargzFile(outputpath, path);*/
		
		file=context.getConfiguration().get(FileInputFormat.INPUT_DIR);
		Configuration conf=new Configuration(); 
		FileSystem fs=FileSystem.get(URI.create(file.toString()),conf);
		FileSystem localfs=FileSystem.get(URI.create("/home/yjw"),conf);
		String loc="/home/yjw/Desktop/NewInput";
		localfs.mkdirs(new Path(loc));
		String fpath=file.toString().trim();
		String filename=fpath.substring(fpath.lastIndexOf('/'), fpath.length());
		String locfile=loc+filename;
		System.out.println("locfile:"+locfile);
		fs.copyToLocalFile(new Path(file), new Path(locfile));
		keyvalue=new LinkedList<String>();
		String path=locfile.substring(0,locfile.lastIndexOf('/'));
		GZip turn=new GZip(locfile);
		String tfile=turn.unTargzFile(locfile, path);
		System.out.println("tfile:"+tfile);
		System.out.println("path:"+path);
	/*	GoldwindToCSV turn2=new GoldwindToCSV();
		turn2.TraversFolder(t);
			
		turn2.ZipToDB(t);
		try {
			turn2.DbToCSV(t);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		*/
		String hdfsoutputpath=fpath.substring(0,fpath.lastIndexOf('/'));
		System.out.println("hdfsoutputpath:"+hdfsoutputpath);
		String fullpath=hdfsoutputpath+tfile.substring(tfile.lastIndexOf('/'), tfile.length());
		if(fs.exists(new Path(fullpath)))
		{
			fs.delete(new Path(fullpath),true);
		}
		fs.copyFromLocalFile(new Path(tfile), new Path(hdfsoutputpath));
		
		System.out.println("finish");
		getFileInfo(fullpath);
		
	}
	
	/*private void mkFolder(String fileName) {
	       File f = new File(fileName);
	       if (!f.exists()) {
	           f.mkdir();
	       }
	    }
	 
	    private File mkFile(String fileName) {
	       File f = new File(fileName);
	       try {
	           f.createNewFile();
	       } catch (IOException e) {
	           e.printStackTrace();
	       }
	       return f;
	    }*/
	public void getFileInfo(String path) throws IOException
	{
		int fileNum = 0, folderNum = 0;
		System.out.print("getFileInfo:"+path);
		LOG.info("getFileInfor  path:"+path);
		Configuration conf=new Configuration();
		FileSystem fs=FileSystem.get(URI.create(path),conf);
		FileStatus[] status=fs.listStatus(new Path(path));
		Queue<FileStatus> q=new LinkedList<FileStatus>();
		for(FileStatus file:status)
	       {
	    	   q.add(file);
	       }
	       
	       while(q.size()>0)
	       {
	    	   FileStatus tmp=q.poll();
	    	   if(tmp.isDirectory())
	    	   {
	    		   System.out.println("文件夹："+tmp.getPath());
	    		   FileStatus[] status2=fs.listStatus(new Path(tmp.getPath().toString()));
	    		   for(FileStatus file2:status2)
	    		   {
	    			   q.add(file2);
	    		   }
	    	   }
	    	   else
	    	   {
	    		   System.out.println("文件："+tmp.getPath());
	    		   turntomap(tmp.getPath().toString());
	    	   }
	       }
		
}
	public void turntomap(String file) throws IOException
	{
		
		//BufferedReader reader =new BufferedReader(new InputStreamReader(new FileInputStream(file),"UTF-8"));
		String name=file.toString();
		String line=null;
		//Map<String,String> tmp=new HashMap<String,String>();
		System.out.println("read file");
		Configuration conf=new Configuration();
		FileSystem fs =FileSystem.get(URI.create(file),conf);
		FSDataInputStream inputStream=fs.open(new Path(file));
		while((line=inputStream.readLine())!=null)
		{
			keyvalue.add(name+"$"+line);
			//System.out.println("line+:"+line);
		}
		System.out.println("end");
	}
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String k=null;
		String v=null;
		boolean lock=false;
		if(keyvalue.size()>0)
		{
			System.out.println("nextKeyValue");
			String line=keyvalue.poll();
			k=line.substring(0, line.indexOf('$'));
			if(line.indexOf('$')+1<line.length())
				v=line.substring(line.indexOf('$')+1,line.length());
			key=new Text(k);
			value=new Text(v);
			lock=true;
		}
		
		
		return lock;
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		System.out.println("kk:"+key);
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		System.out.println("vv:"+value);
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

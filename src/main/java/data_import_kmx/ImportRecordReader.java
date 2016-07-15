package data_import_kmx;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.io.BufferedReader; 
import java.io.File; 
import java.io.FileNotFoundException; 
import java.io.FileReader; 
import java.io.IOException; 
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
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
	
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		System.out.println("gergergergergr:"+context.getConfiguration().get(FileInputFormat.INPUT_DIR));
		file=context.getConfiguration().get(FileInputFormat.INPUT_DIR);
		//GZip test=new GZip(file);
		String outputpath=file.substring(0, file.lastIndexOf('/'));
		GZip turn=new GZip("/home/yjw/Desktop/test.tar.gz");
		String t=turn.unTargzFile("/home/yjw/Desktop/test.tar.gz", "/home/yjw/Desktop");
		System.out.println("desk:"+t);
		keyvalue=new LinkedList<String>();
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
		
		getFileInfo(t);
		
	}
	public void getFileInfo(String path) throws IOException
	{
		int fileNum = 0, folderNum = 0;
        File file = new File(path);
        if (file.exists()) {
            LinkedList<File> list = new LinkedList<File>();
            File[] files = file.listFiles();
            for (File file2 : files) {
                if (file2.isDirectory()) {
                    System.out.println("文件夹:" + file2.getAbsolutePath());
                    list.add(file2);
                    fileNum++;
                } else {
                    System.out.println("文件:" + file2.getAbsolutePath());
                    turntomap(file2);
                    folderNum++;
                }
            }
            File temp_file;
            while (!list.isEmpty()) {
                temp_file = list.removeFirst();
                files = temp_file.listFiles();
                for (File file2 : files) {
                    if (file2.isDirectory()) {
                        System.out.println("文件夹:" + file2.getAbsolutePath());
                        list.add(file2);
                        fileNum++;
                    } else {
                        System.out.println("文件:" + file2.getAbsolutePath());
                        folderNum++;
                    }
                }
            }
        } else {
            System.out.println("文件不存在!");
        }
        
      /*  while(keyvalue.size()!=0)
        {
        	System.out.println("show:"+keyvalue.poll());
        }*/
        System.out.println("文件夹共有:" + folderNum + ",文件共有:" + fileNum);

}
	public void turntomap(File file) throws IOException
	{
		BufferedReader reader =new BufferedReader(new InputStreamReader(new FileInputStream(file),"UTF-8"));
		String name=file.toString();
		String line=null;
		//Map<String,String> tmp=new HashMap<String,String>();
		System.out.println("read file");
		
		while((line=reader.readLine())!=null)
		{
			//tmp.put(name, line);
			//keyvalue.add(tmp);
			keyvalue.add(name+"$"+line);

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

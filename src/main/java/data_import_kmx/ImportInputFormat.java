package data_import_kmx;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.StopWatch;


public class ImportInputFormat extends FileInputFormat<Text, Text> {

	public static final Log LOG = LogFactory.getLog(ImportInputFormat.class);
	public List<InputSplit> getSplits(JobContext job) throws IOException {	
		StopWatch sw=new StopWatch().start();
		List<InputSplit> splits=new ArrayList<InputSplit>();
		List<FileStatus> files=listStatus(job);
		FileStatus file;
		for(int i=0;i<files.size();i++)
		{
			file=files.get(i);
			Path path=file.getPath();
		//	System.out.println("LOG~~~~~~~~~~~~~~~~~`:"+path);
		//	LOG.info("LOG~~~~~~~~~~~~~~~~~`:"+path);
			long length=file.getLen();
			if(length!=0&&path.toString().contains(".tar.gz"))
			{
				BlockLocation[] blkLocations;
				if(file instanceof LocatedFileStatus)
				{
					blkLocations = ((LocatedFileStatus) file).getBlockLocations();
				}
				else
				{
					FileSystem fs = path.getFileSystem(job.getConfiguration());
			        blkLocations = fs.getFileBlockLocations(file, 0, length);
				}
				splits.add(makeSplit(path,0,length,blkLocations[0].getHosts(),blkLocations[0].getCachedHosts()));
			//	System.out.println("splits:"+splits.toString());
			}
		}
		System.out.println("end:"+splits);
		
	
		return splits;
	}
	
	@Override
	public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return new ImportRecordReader();
	}

}

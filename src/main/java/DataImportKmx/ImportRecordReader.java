package DataImportKmx;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

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
		System.out.println(context.getConfiguration().get(FileInputFormat.INPUT_DIR));
		file = split.toString().substring(0, split.toString().lastIndexOf(':')).trim();
		System.out.println("file:" + file);
		outpath = context.getConfiguration().get(FileOutputFormat.OUTDIR);
		String file1 = file;
		Configuration conf = new Configuration();
		File directory = new File("");
		FileSystem fs = FileSystem.get(URI.create(file1.toString()), conf);
		String loc = directory.getAbsolutePath() + "/tmp";
		File dir = new File(loc);
		dir.mkdir();
		String fpath = file1.toString().trim();
		String filename = fpath.substring(fpath.lastIndexOf('/'), fpath.length());
		String locFile = loc + filename;
		fs.copyToLocalFile(new Path(file1), new Path(locFile));
		keyvalue = new LinkedList<String>();
		String path = locFile.substring(0, locFile.lastIndexOf('/'));
		GZip turn = new GZip(locFile);
		String tfile = turn.unTargzFile(locFile, path);
		LOG.info("ImportRecordReader:" + locFile + "  path:" + path);
		keyvalue.add(outpath + "$" + tfile);
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

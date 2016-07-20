package data_import_kmx;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ImportJob {

	private Configuration conf;
	private Job job;
	private String inputPath = null;
	private String outputPath = null;
	private String csvfile = null;

	public ImportJob(String input, String csv, String output) throws IOException {
		System.out.println("importjob-importjob");
		conf = new Configuration();
		// job=Job.getInstance(conf,"import-data-kmx");
		inputPath = input;
		outputPath = output;
		csvfile = csv;
		setMRJobConf();
	}

	/**
	 * init ImportJob
	 * 
	 * @param conf
	 *            configuration
	 * @return return init result
	 */
	public boolean init(Configuration conf) {

		return false;
	}

	/**
	 * delete Outputdir
	 * 
	 * @param dir
	 * @return
	 */
	public boolean deleteDir(File dir) {
		System.out.println("ImportJob-deleteDir");
		if (dir.isDirectory()) {
			String[] children = dir.list();
			for (int i = 0; i < children.length; i++) {
				boolean success = deleteDir(new File(dir, children[i]));
				if (!success) {
					return false;
				}
			}
		}
		return dir.delete();
	}

	/**
	 * run a ImportJob
	 * 
	 * @param conf
	 *            configuration
	 * @return return Job's finish or unfinish
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	public boolean run() throws IOException, ClassNotFoundException, InterruptedException {
		System.out.println("importJob-run");
		// deleteDir(new File("/home/yjw/Desktop/output"));
		job = Job.getInstance(conf, "import data kmx");
		job.setJarByClass(data_import_kmx.class);
		// FileInputFormat.addInputPath(job, new
		// Path("/home/yjw/Desktop/test.tar.gz"));
		// FileInputFormat.addInputPath(job, new
		// Path("/home/yjw/Desktop/input/mrtest.csv"));
		// FileOutputFormat.setOutputPath(job, new
		// Path("/home/yjw/Desktop/output"));
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		FileInputFormat.addInputPath(job, new Path(csvfile));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return false;
	}

	/**
	 * set MR Job configuration Jobname、map class、map output key、map output
	 * value、combine class、reduce class
	 * 
	 */

	private void setMRJobConf() {
		conf.set(MRJobConfig.JOB_NAME, "ImportDataToKMX");
		conf.set(MRJobConfig.MAP_CLASS_ATTR, ImportMapper.class.getName());
		conf.set(MRJobConfig.INPUT_FORMAT_CLASS_ATTR, ImportInputFormat.class.getName());
		conf.set(MRJobConfig.MAP_OUTPUT_KEY_CLASS, Text.class.getName());
		// conf.set(MRJobConfig.MAP_OUTPUT_VALUE_CLASS, Text.class.getName());

		conf.set(MRJobConfig.MAP_OUTPUT_VALUE_CLASS, LList.class.getName());
		conf.set(MRJobConfig.OUTPUT_KEY_CLASS, Text.class.getName());
		conf.set(MRJobConfig.OUTPUT_VALUE_CLASS, Text.class.getName());
		// conf.set(MRJobConfig.COMBINE_CLASS_ATTR,
		// ImportReduce.class.getName());
		conf.set(MRJobConfig.REDUCE_CLASS_ATTR, ImportReduce.class.getName());

	}

}

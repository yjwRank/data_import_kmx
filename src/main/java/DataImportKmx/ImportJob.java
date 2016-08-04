package DataImportKmx;

import java.io.IOException;
import java.net.URI;
import java.util.LinkedList;
import java.util.Queue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ImportJob {
	public static final Log LOG = LogFactory.getLog(ImportJob.class);
	private Configuration conf;
	private Job job;
	private String inputPath = null;
	private String outputPath = null;
	private String csvAndPropertiesPath = null;

	public ImportJob(String input, String csvAndProperties, String output) throws IOException {
		System.out.println("importjob-importjob");
		LOG.info("ImportJob constructor  input:" + input + " analysisfile:" + csvAndProperties + " outputPath:"
				+ output);
		conf = new Configuration();
		// conf.addResource(new URL("http://192.168.130.120:8088/conf"));
		inputPath = input;
		outputPath = output;
		csvAndPropertiesPath = csvAndProperties;
		setMRJobConf();
	}

	/**
	 * run a ImportJob add inputpath、set outputpath、set distributed Cache file
	 * 
	 * @param conf
	 *            configuration
	 * @return return Job's finish or unfinish
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	public boolean run() throws IOException, ClassNotFoundException, InterruptedException {
		System.out.println("impo3rtJob-run");
		LOG.info("ImportJob-run");
		job = Job.getInstance(conf, "import data kmx");
		job.setJarByClass(DataImportKmx.class);
		FileSystem fs = FileSystem.get(URI.create(csvAndPropertiesPath), conf);
		Queue<FileStatus> list = new LinkedList<FileStatus>();
		FileStatus file = fs.getFileStatus(new Path(csvAndPropertiesPath));
		list.add(file);
		while (list.size() > 0) {
			FileStatus tmp = list.poll();
			if (tmp.isDirectory()) {
				FileStatus[] status = fs.listStatus(new Path(tmp.getPath().toString()));
				for (FileStatus file2 : status)
					list.add(file2);
			} else {
				job.addCacheFile(new Path(tmp.getPath().toString()).toUri());
				LOG.info("ImportJob-run cacheFile:" + tmp.getPath().toString());
			}
		}
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		LOG.info("ImportJob-run  InputPath:" + inputPath + " OutputPath:" + outputPath);
		FileInputFormat.setInputDirRecursive(job, true);
		job.waitForCompletion(true);
		renameFile();
		return false;
	}

	/**
	 * rename the result file rm **-r-**
	 * 
	 * @throws IOException
	 */
	public void renameFile() throws IOException {
		LOG.info("ImportJob-rename");
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(outputPath), conf);
		FileStatus[] status = fs.listStatus(new Path(outputPath));
		Queue<FileStatus> q = new LinkedList<FileStatus>();
		for (FileStatus f : status) {
			q.add(f);
		}
		while (q.size() > 0) {
			FileStatus file = q.poll();
			if (file.isDirectory()) {
				FileStatus[] status2 = fs.listStatus(new Path(file.getPath().toString()));
				for (FileStatus f2 : status2) {
					q.add(f2);
				}
			} else {
				String name = file.getPath().toString();
				String filename = name;

				if (filename.contains("part")) {
					fs.delete(new Path(name));
				} else if (filename.contains("-r-")) {
					fs.rename(new Path(name), new Path(filename.substring(0, filename.indexOf('.')) + ".csv"));
				} else if (filename.contains("SUCCESS")) {
					fs.delete(new Path(name));
				}
			}

		}
	}

	/**
	 * set MR Job configuration Jobname、map class、map output key、map output
	 * value
	 * 
	 */

	private void setMRJobConf() {
		LOG.info("ImportJob-setMRJobConf");
		conf.set(MRJobConfig.JOB_NAME, "ImportDataToKMX");
		conf.set(MRJobConfig.MAP_CLASS_ATTR, ImportMapper.class.getName());
		conf.set(MRJobConfig.INPUT_FORMAT_CLASS_ATTR, ImportInputFormat.class.getName());
		conf.set(MRJobConfig.MAP_OUTPUT_KEY_CLASS, Text.class.getName());
		conf.set(MRJobConfig.MAP_OUTPUT_VALUE_CLASS, Text.class.getName());
	}

}

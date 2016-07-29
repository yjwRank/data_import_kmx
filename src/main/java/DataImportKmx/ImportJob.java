package DataImportKmx;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.LinkedList;
import java.util.Queue;
public class ImportJob {

	private Configuration conf;
	private Job job;
	private String inputPath = null;
	private String outputPath = null;
	private String csvfile = null;

	public ImportJob(String input, String csv, String output) throws IOException {
		System.out.println("importjob-importjob");
		conf = new Configuration();
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
		job = Job.getInstance(conf, "import data kmx");
		job.setJarByClass(DataImportKmx.class);
		FileSystem fs=FileSystem.get(URI.create(csvfile),conf);
		Queue<FileStatus> list=new LinkedList<FileStatus>();
		FileStatus file=fs.getFileStatus(new Path(csvfile));
		list.add(file);
		while(list.size()>0)
		{
			FileStatus tmp=list.poll();
			if(tmp.isDirectory())
			{
				FileStatus[] status=fs.listStatus(new Path(tmp.getPath().toString()));
				for(FileStatus file2:status)
					list.add(file2);
			}
			else
			{
				job.addCacheFile(new Path(tmp.getPath().toString()).toUri());
			}
		}
		//job.addCacheFile(new Path(csvfile).toUri());
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		FileInputFormat.addInputPath(job, new Path(csvfile));
		FileInputFormat.setInputDirRecursive(job, true);
		int exit = job.waitForCompletion(true) ? 0 : 1;
		renameFile();

		//System.exit(exit == 0 ? 0 : 1);
		return false;
	}

	public void renameFile() throws IOException {
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

				if (filename.contains("/err-2")) {
					if (file.getLen() == 0) {
						fs.delete(new Path(name));
					}
				} else if (filename.contains("err-r")) {
					fs.rename(new Path(name), new Path(filename.substring(0, filename.lastIndexOf('/')) + "err"));
				} else if (filename.contains("part")) {
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
	 * value、combine class、reduce class
	 * 
	 */

	private void setMRJobConf() {
		conf.set(MRJobConfig.JOB_NAME, "ImportDataToKMX");
		conf.set(MRJobConfig.MAP_CLASS_ATTR, ImportMapper.class.getName());
		conf.set(MRJobConfig.INPUT_FORMAT_CLASS_ATTR, ImportInputFormat.class.getName());
		conf.set(MRJobConfig.MAP_OUTPUT_KEY_CLASS, Text.class.getName());
		conf.set(MRJobConfig.MAP_OUTPUT_VALUE_CLASS, Text.class.getName());

		// conf.set(MRJobConfig.MAP_OUTPUT_VALUE_CLASS, LList.class.getName());
		conf.set(MRJobConfig.OUTPUT_KEY_CLASS, Text.class.getName());
		conf.set(MRJobConfig.OUTPUT_VALUE_CLASS, Text.class.getName());
		conf.set(MRJobConfig.REDUCE_CLASS_ATTR, ImportReduce.class.getName());
		// conf.set("mapred.child.java.opts", "-Xmx2048m");
	}

}

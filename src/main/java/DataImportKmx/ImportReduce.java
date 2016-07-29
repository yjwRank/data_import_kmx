package DataImportKmx;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * import job Reducer
 * 
 * @author yjw
 *
 */
public class ImportReduce extends Reducer<Text, LList, NullWritable, Text> {
	private IntWritable result = new IntWritable();
	private IntWritable own = new IntWritable();
	private MultipleOutputs<NullWritable, Text> mos;
	public static final Log LOG = LogFactory.getLog(ImportReduce.class);
	private String err = null;
	private String errType = null;

	protected void setup(Reducer<Text, LList, NullWritable, Text>.Context context) {
		mos = new MultipleOutputs<NullWritable, Text>(context);
	}

	protected void cleanup(Reducer<Text, LList, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		mos.close();
	}

	public String converToISOTime(String tuibineID) throws ParseException {
		DateFormat turbineID_format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date turbineID_date = turbineID_format.parse(tuibineID);
		TimeZone tz = TimeZone.getTimeZone("UTC");
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
		df.setTimeZone(tz);
		turbineID_format.setLenient(false);
		String ISOTime = df.format(turbineID_date);

		Date now = new Date();
		if (turbineID_date.after(now))
			return null;

		try {
			turbineID_format.parse(tuibineID);
			return ISOTime;
		} catch (Exception e) {
			return null;
		}
		// return ISOTime;
	}

	public void reduce(Text key, Iterable<LList> title, Context context) throws IOException, InterruptedException {
		System.out.println("RR");
		for (LList tit : title) {

			String buffer = "";
			// String line = key.toString();
			String line = tit.getLine();
			// System.out.println("line:"+line);
			String[] token1 = line.split(",");
			Vector<String> vec = new Vector<String>();
			if (token1.length != tit.size()) {
				System.out.println("error " + token1.length + " " + tit.size() + "  name:" + tit.getname());
			} else {
				// System.out.println("right:" + token1.length + " name:" +
				// tit.getname());
				for (int i = 0; i < token1.length; i++) {
					vec.add("0");
				}
				for (int i = 0; i < tit.size(); i++) {
					vec.setElementAt(token1[i], tit.get(i));
				}
			}

			if (vec.size() > 0) {

				if (tit.getWMAN_Tm() == true) {
					buffer += vec.get(0);
					int i = 1;
					try {
						String tmp = converToISOTime(vec.get(i));
						if (tmp == null) {
							String name = tit.getname();
							errType = "not a date or after Now";
							String errmessage = line + "  in file:" + name + " errType:" + errType;
							mos.write(NullWritable.get(), new Text(errmessage),
									name.substring(0, name.lastIndexOf('/')) + "/err");
							return;
						}
						buffer += ',';
						buffer += tmp;
						i++;
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					for (; i < vec.size(); i++) {
						buffer += ",";
						buffer += vec.get(i);
					}

					mos.write(NullWritable.get(), new Text(buffer), tit.getname());
				} else {
					LOG.error("err item");
				}

			} else {
				LOG.error("vec size 0");
			}

			// mos.write(NullWritable.get(), new Text(buffer),
			// "/home/yjw/Desktop/output/test.csv");
			// LOG.info("*********:" + key + " tit:" + tit.getname());

			// break;
		}

	}
}
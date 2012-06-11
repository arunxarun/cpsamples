package org.arunxarun.mapreduce.sample.cp;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * @author arun.x.arun@gmail.com 
 * get view counts by user
 * 
 */
public class GroupViewsByUser extends Configured implements Tool {

	private static Logger LOGGER = Logger.getLogger(GroupViewsByUser.class);
	public static final String INTERNAL_DELIMITER = "="; // for breaking up fields
	public static final String FIELD_DELIMITER = "\t"; // for breaking up lines.
	enum GroupByUserCounters {
		BAD_LOG_FORMAT, BAD_URI_SUBFORMAT, MAP_CONTEXT_WRITE_IO_EXCEPTION, MAP_CONTEXT_WRITE_INTERRUPTED_EXCEPTION, REDUCE_CONTEXT_WRITE_IO_EXCEPTION, REDUCE_CONTEXT_WRITE_INTERRUPTED_EXCEPTION, BAD_REDUCE_INPUT_FORMAT
	}

	/**
	 * the key the user, the value is the URI
	 * 
	 */
	public static class GroupViewsByUserMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(final LongWritable key, final Text value,
				final Context context) {
			// assume common log format:
			// 127.0.0.1\t-\tfrank\t[10/Oct/2000:13:55:36
			// -0700]\t"GET /apache_pb.gif HTTP/1.0"\t200\t2326\n
			String raw = value.toString();

			String fields[] = raw.split("\t");

			if (fields.length < 6 || fields.length > 7) {
				context.getCounter(GroupByUserCounters.BAD_LOG_FORMAT)
						.increment(1);
				return;
			}

			String userIdText = fields[2];
			String rawHttpInfo = fields[4];

			String uriComponents[] = rawHttpInfo.split(" ");

			if (uriComponents.length < 3) {
				context.getCounter(GroupByUserCounters.BAD_URI_SUBFORMAT)
						.increment(1);
				return;
			}
			Text userId = new Text(userIdText);
			Text uri = new Text(uriComponents[1]);

			try {
				context.write(userId, uri);
			} catch (IOException e) {
				context.getCounter(
						GroupByUserCounters.MAP_CONTEXT_WRITE_IO_EXCEPTION)
						.increment(1);
				e.printStackTrace();
			} catch (InterruptedException e) {
				context.getCounter(
						GroupByUserCounters.MAP_CONTEXT_WRITE_INTERRUPTED_EXCEPTION)
						.increment(1);
				e.printStackTrace();
			}

		}
	}

	/**
	 * emit user: uri1:count1...uriN:countN
	 * 
	 */
	public static class GroupViewsByUserReducer extends
			Reducer<Text, Text, Text, Text> {

		

		/**
		 *
		 */
		@Override
		public void reduce(Text key, Iterable<Text> input, Context context) {

			Map<Text, Integer> URItoCount = new HashMap<Text, Integer>();
			Iterator<Text> it = input.iterator();

			// aggregate views by uri.
			while (it.hasNext()) {
				Text value = new Text(it.next());
				LOGGER.info("processing "+value.toString());
				Integer count = URItoCount.get(value);

				if (count == null) {
					URItoCount.put(value, Integer.valueOf(1));
				} else {
					URItoCount.put(value, Integer.valueOf(count + 1));
				}

			}

			// emit view:count format, tab delimited.

			StringBuffer sbuf = new StringBuffer();
			LOGGER.info("processed entry length = "+URItoCount.size());
			for (Map.Entry<Text,Integer> entry : URItoCount.entrySet()) {
				LOGGER.info("building view: processing "+entry.getKey().toString());
				sbuf.append(entry.getKey().toString()).append(INTERNAL_DELIMITER)
						.append(entry.getValue().toString());
				sbuf.append(FIELD_DELIMITER);
			}

			// convert list to hadoop Text
			String viewBuffer = sbuf.toString();
			LOGGER.info("reduce: key = " + key.toString() + ", value = "
					+ viewBuffer);

			Text allViews = new Text(viewBuffer);

			try {
				context.write(key, allViews);
			} catch (IOException e) {
				LOGGER.error(e);
				context.getCounter(
						GroupByUserCounters.REDUCE_CONTEXT_WRITE_IO_EXCEPTION)
						.increment(1);
			} catch (InterruptedException e) {
				LOGGER.error(e);
				context.getCounter(
						GroupByUserCounters.REDUCE_CONTEXT_WRITE_INTERRUPTED_EXCEPTION)
						.increment(1);

			}
		}
	}

	/**
	 * 
	 */
	@Override
	public int run(String[] args) throws Exception {
		String className = this.getClass().getName();
		// return non-zero if error
		int result = 1;

		LOGGER.debug("Starting Job:" + className);

		try {
			Configuration conf = new Configuration();
			Job job = new Job(conf, className);

			job.setJarByClass(this.getClass());
			job.setMapperClass(GroupViewsByUser.GroupViewsByUserMapper.class);
			job.setReducerClass(GroupViewsByUser.GroupViewsByUserReducer.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class); // this one matches the
													// exected map output!
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));

			job.waitForCompletion(true);

		} catch (Exception e) {
			LOGGER.error(
					"Overall Job did not complete succesfully" + className, e);
			throw e;
		}
		return result;

	}

	/**
	 * entry point
	 * 
	 * @param args
	 */
	public static void main(final String[] args) {
		try {
			System.exit(ToolRunner.run(new GroupViewsByUser(), args));
		} catch (Exception e) {
			LOGGER.error("Exception occured.", e);
			System.exit(1);
		}
	}
}

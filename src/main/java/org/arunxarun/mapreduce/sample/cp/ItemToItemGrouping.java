package org.arunxarun.mapreduce.sample.cp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import org.arunxarun.mapreduce.sample.cp.GroupViewsByUser.GroupByUserCounters;

/**
 * @author arun.x.arun@gmail.com 
 * get view counts by user
 * 
 */
public class ItemToItemGrouping extends Configured implements Tool {

	private static Logger LOGGER = Logger.getLogger(ItemToItemGrouping.class);
	public static final String INTERNAL_DELIMITER = "=";
	public static final String FIELD_DELIMITER = "\t"; // for breaking up lines.
	enum ItemToItemCounters {
		BAD_MAP_INPUT_FORMAT, BAD_URI_SUBFORMAT, MAP_CONTEXT_WRITE_IO_EXCEPTION, MAP_CONTEXT_WRITE_INTERRUPTED_EXCEPTION, REDUCE_CONTEXT_WRITE_IO_EXCEPTION, REDUCE_CONTEXT_WRITE_INTERRUPTED_EXCEPTION, BAD_REDUCE_INPUT_FORMAT
	}

	/**
	 * the key the user, the value is the URI
	 * 
	 */
	public static class ItemToItemGroupingMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(final LongWritable key, final Text value,
				final Context context) {
			// this job takes the input from GroupViewsByUser
			// user1\turiX:3\turiY:4\turiZ:12
			String raw = value.toString();
			
			
			String fields[] = raw.split("\t");

			if(fields.length == 1) {
				context.getCounter(ItemToItemCounters.BAD_MAP_INPUT_FORMAT)
				.increment(1);
				return;
			}
			// fields[0] is the user ID, we skip that for item-item co-occurrence.
			for(int i = 1; i < fields.length;i++) {
				// permute all combinations of co-occurrences
				
				for(int j = 1;j < fields.length;j++) {
					if(j == i) {
						continue; // dont count self co-occurrences
					}
					else {
						String partsI[] = fields[i].split(GroupViewsByUser.INTERNAL_DELIMITER);
						String partsJ[] = fields[j].split(GroupViewsByUser.INTERNAL_DELIMITER);
						
						if(partsI.length != 2 || partsJ.length != 2) {
							context.getCounter(ItemToItemCounters.BAD_MAP_INPUT_FORMAT)
							.increment(1);
							return;
						}
						try {
							LOGGER.info(partsI[0]+"=>"+partsJ[0]);
							context.write(new Text(partsI[0]),new Text(partsJ[0]));
						} catch (IOException e) {
							context.getCounter(
									ItemToItemCounters.MAP_CONTEXT_WRITE_IO_EXCEPTION)
									.increment(1);
							e.printStackTrace();
						} catch (InterruptedException e) {
							context.getCounter(
									ItemToItemCounters.MAP_CONTEXT_WRITE_INTERRUPTED_EXCEPTION)
									.increment(1);
							e.printStackTrace();
						}
					}
					
					
				}
			}
			

		}
	}

	/**
	 * emit user: uri1:count1...uriN:countN
	 * 
	 */
	public static class ItemToItemGroupingReducer extends
			Reducer<Text, Text, Text, Text> {

		/**
		 *
		 */
		@Override
		public void reduce(Text key, Iterable<Text> input, Context context) {

			Map<Text, Integer> URItoCount = new HashMap<Text, Integer>();
			Iterator<Text> it = input.iterator();
			long totalCount = 0;
			// these are co-occurrences: sum them.
			while (it.hasNext()) {
				Text value = new Text(it.next());

				Integer count = URItoCount.get(value);

				if (count == null) {
					URItoCount.put(value, Integer.valueOf(1));
				} else {
					URItoCount.put(value, Integer.valueOf(count + 1));
				}

				totalCount++;
			}

			// emit tab delimited and from greatest to least
			// uri2=prob(uri2 | uri1) uri3=prob(uri3 | uri1)

			Map<Double,List<Text>> byProb = new HashMap<Double,List<Text>>();
			List<Double> sortedSet = new ArrayList<Double>();
			
			for (Map.Entry<Text, Integer> entry : URItoCount.entrySet()) {
				double probability = ((double)entry.getValue())/totalCount;
				List<Text> list = byProb.get(probability);
				
				if(list == null) {
					list = new ArrayList<Text>();
					byProb.put(probability,list);
					sortedSet.add(probability);
				}
				list.add(entry.getKey());
			}

			// sort from greatest to least.
			Collections.sort(sortedSet);  
			Collections.reverse(sortedSet);
			
			// emit uri:value where value = count / total 
			StringBuffer sbuf = new StringBuffer();
			
			for(double probability : sortedSet){
				List<Text> uris = byProb.get(probability);
				
				for(Text uri : uris) {
					sbuf.append(uri.toString()).append(INTERNAL_DELIMITER).append(probability).append(FIELD_DELIMITER);
				}
			}
				
			// convert list to hadoop Text
			String coOccurrenceBuffer = sbuf.toString();
			LOGGER.debug("reduce: key = " + key.toString() + ", value = "
					+ coOccurrenceBuffer);

			Text allViews = new Text(coOccurrenceBuffer);

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
			job.setMapperClass(ItemToItemGrouping.ItemToItemGroupingMapper.class);
			job.setReducerClass(ItemToItemGrouping.ItemToItemGroupingReducer.class);

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
			System.exit(ToolRunner.run(new ItemToItemGrouping(), args));
		} catch (Exception e) {
			LOGGER.error("Exception occured.", e);
			System.exit(1);
		}
	}
}

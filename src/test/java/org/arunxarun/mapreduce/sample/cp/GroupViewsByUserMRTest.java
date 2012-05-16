package org.arunxarun.mapreduce.sample.cp;

import static org.junit.Assert.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.junit.Test;

import org.arunxarun.mapreduce.unitmocks.MockCounter;
import org.arunxarun.mapreduce.unitmocks.MockInputSplit;
import org.arunxarun.mapreduce.unitmocks.MockOutputCommitter;
import org.arunxarun.mapreduce.unitmocks.MockRawKeyValueIterator;
import org.arunxarun.mapreduce.unitmocks.MockRecordReader;
import org.arunxarun.mapreduce.unitmocks.MockRecordWriter;
import org.arunxarun.mapreduce.unitmocks.MockStatusReporter;

/**
 * @author arun.x.arun@gmail.com 
 * unit tests for GroupViewsByUser
 * 
 */
public class GroupViewsByUserMRTest {

	@Test
	public void testMapperValidInput() throws IOException, InterruptedException {

		GroupViewsByUser.GroupViewsByUserMapper mapper = new GroupViewsByUser.GroupViewsByUserMapper();

		MockRecordWriter<Text, Text> rw = new MockRecordWriter<Text, Text>();
		Mapper<LongWritable, Text, Text, Text>.Context context = getMapperContext(
				mapper, rw);

		LongWritable key = new LongWritable(1);
		Text value = new Text(
				"127.0.0.1\t-\tfrank\t[10/Oct/2012:13:55:36 -0700]\t\"GET /products/prod2.html HTTP/1.0\"\t200\t1002\n");
		mapper.map(key, value, context);
		LongWritable key2 = new LongWritable(2);
		Text value2 = new Text(
				"127.0.0.1\t-\tjoe\t[10/Oct/2012:14:35:22 -0700]\t\"GET /products/prod1.html HTTP/1.0\"\t200\t2326\n");
		mapper.map(key2, value2, context);
		Map<Text, Text> map = rw.getMap();
		assertNotNull(map);
		assertTrue(map.size() == 2);
		assertNotNull(map.get(new Text("joe")));
		assertNotNull(map.get(new Text("frank")));

	}

	@Test
	public void testMapperInvalidFormattedInput() throws IOException,
			InterruptedException {
		GroupViewsByUser.GroupViewsByUserMapper mapper = new GroupViewsByUser.GroupViewsByUserMapper();

		MockRecordWriter<Text, Text> rw = new MockRecordWriter<Text, Text>();
		Mapper<LongWritable, Text, Text, Text>.Context context = getMapperContext(
				mapper, rw);

		LongWritable key = new LongWritable(1);
		Text value = new Text(
				"1294203732 2011-01-05 00:02:12 2682301003077099295 6917530613557758086 H.15.1 N");
		mapper.map(key, value, context);

		assertEquals(
				1,
				context.getCounter(
						GroupViewsByUser.GroupByUserCounters.BAD_LOG_FORMAT)
						.getValue());

	}

	@Test
	public void testReducerValidInput() throws IOException,
			InterruptedException {

		GroupViewsByUser.GroupViewsByUserReducer reducer = new GroupViewsByUser.GroupViewsByUserReducer();

		Configuration conf = new Configuration();
		TaskAttemptID taskId = new TaskAttemptID("foo", 1, true, 1, 12);
		MockRecordWriter<Text, Text> dwr = new MockRecordWriter<Text, Text>();
		
		Reducer<Text, Text, Text, Text>.Context context = getReducerContext(reducer,dwr,conf,taskId); 
			
//			reducer.new Context(
//				conf, taskId, drkv, new MockCounter(), new MockCounter(), dwr,
//				doc, dsr, null, Text.class, Text.class);

		Iterable<Text> input = new Iterable<Text>() {

			@Override
			public Iterator<Text> iterator() {
				List<Text> list = new ArrayList<Text>();

				list.add(new Text("http://foobar.com"));
				list.add(new Text("http://foobar.com"));
				list.add(new Text("http://foobar.com"));
				list.add(new Text("http://foobar.com"));

				return list.iterator();
			}

		};

		Text key = new Text("userkey");

		reducer.reduce(key, input, context);

		Map<Text, Text> map = dwr.getMap();

		assertNotNull(map);
		assertTrue(map.size() == 1);
		assertNotNull(map.get(new Text("userkey")));
		Text value = map.get(key);
		assertEquals("http://foobar.com:4\t", value.toString());

	}

	/**
	 * return a valid mapper context.
	 * 
	 * @param mapper
	 * @param rw
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private Mapper<LongWritable, Text, Text, Text>.Context getMapperContext(
			Mapper<LongWritable, Text, Text, Text> mapper,
			MockRecordWriter<Text, Text> rw) throws IOException,
			InterruptedException {
		Configuration conf = new Configuration();
		TaskAttemptID taskId = new TaskAttemptID();

		Mapper<LongWritable, Text, Text, Text>.Context context = mapper.new Context(
				conf, taskId, new MockRecordReader<LongWritable, Text>(), rw,
				new MockOutputCommitter(), new MockStatusReporter(),
				new MockInputSplit());

		return context;

	}
	
	/**
	 * 
	 * return a valid reducer context
	 * @param reducer
	 * @param dwr
	 * @param conf
	 * @param id
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private Reducer<Text,Text,Text,Text>.Context getReducerContext(
			Reducer<Text,Text,Text,Text> reducer, 
			MockRecordWriter<Text, Text> dwr,
			Configuration conf, 
			TaskAttemptID id) throws IOException, 
			InterruptedException {
		
		MockOutputCommitter doc = new MockOutputCommitter();
		MockStatusReporter dsr = new MockStatusReporter();
		MockRawKeyValueIterator drkv = new MockRawKeyValueIterator();
		
		
		Reducer<Text,Text,Text,Text>.Context context = reducer.new Context(
		conf, id, drkv, new MockCounter(), new MockCounter(), dwr,
		doc, dsr, null, Text.class, Text.class);
		
		return context;
	}
	
	
}

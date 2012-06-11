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
public class ItemToItemGroupingMRTest {

	@Test
	public void testMapperValidInput() throws IOException, InterruptedException {

		ItemToItemGrouping.ItemToItemGroupingMapper mapper = new ItemToItemGrouping.ItemToItemGroupingMapper();

		MockRecordWriter<Text, Text> rw = new MockRecordWriter<Text, Text>();
		Mapper<LongWritable, Text, Text, Text>.Context context = getMapperContext(
				mapper, rw);

		LongWritable key = new LongWritable(1);
		Text value = new Text("frank"+GroupViewsByUser.FIELD_DELIMITER+"/foo/bar.html"+GroupViewsByUser.INTERNAL_DELIMITER+"1"+GroupViewsByUser.FIELD_DELIMITER+"/foo/baz.html"+GroupViewsByUser.INTERNAL_DELIMITER+"1"+GroupViewsByUser.FIELD_DELIMITER+"/foo/car.html"+GroupViewsByUser.INTERNAL_DELIMITER+"1\n");
		mapper.map(key, value, context);
		Map<Text, List<Text>> mapOfLists = rw.getMapOfLists();
		assertNotNull(mapOfLists);
		assertTrue(mapOfLists.size() == 3);
		
		for(Map.Entry<Text,List<Text>> entry : mapOfLists.entrySet()) {
			
			String sortKey = entry.getKey().toString();
			
			List<Text> values = entry.getValue();
			assertEquals(2,values.size());
			for(Text matchedValue : values) {
				
				assertFalse(sortKey.equals(matchedValue.toString()));  // no self sorting
			}
			
			
		}
		
		

	}

	@Test
	public void testMapperInvalidFormattedInput() throws IOException,
			InterruptedException {
		ItemToItemGrouping.ItemToItemGroupingMapper mapper = new ItemToItemGrouping.ItemToItemGroupingMapper();

		MockRecordWriter<Text, Text> rw = new MockRecordWriter<Text, Text>();
		Mapper<LongWritable, Text, Text, Text>.Context context = getMapperContext(
				mapper, rw);

		LongWritable key = new LongWritable(1);
		Text value = new Text(
				"joe     /foo/bar.html#1 /foo/baz.html?1 /foo/car.html:1");
		mapper.map(key, value, context);

		assertEquals(
				1,
				context.getCounter(
						ItemToItemGrouping.ItemToItemCounters.BAD_MAP_INPUT_FORMAT).getValue());

	}

	@Test
	public void testReducerValidInput() throws IOException,
			InterruptedException {

		ItemToItemGrouping.ItemToItemGroupingReducer reducer = new ItemToItemGrouping.ItemToItemGroupingReducer();

		Configuration conf = new Configuration();
		TaskAttemptID taskId = new TaskAttemptID("foo", 1, true, 1, 12);
		MockRecordWriter<Text, Text> dwr = new MockRecordWriter<Text, Text>();
		
		Reducer<Text, Text, Text, Text>.Context context = getReducerContext(reducer,dwr,conf,taskId); 
			

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
		
		String fields[] = value.toString().split(ItemToItemGrouping.INTERNAL_DELIMITER);
		
		assertEquals(2,fields.length);
		
		assertEquals("http://foobar.com",fields[0]);
		assertEquals("1.0\t",fields[1]);

	}

	
	@Test
	public void testReducerValidMultipleInput() throws IOException, InterruptedException {
		
		ItemToItemGrouping.ItemToItemGroupingReducer reducer = new ItemToItemGrouping.ItemToItemGroupingReducer();

		Configuration conf = new Configuration();
		TaskAttemptID taskId = new TaskAttemptID("foo", 1, true, 1, 12);
		MockRecordWriter<Text, Text> dwr = new MockRecordWriter<Text, Text>();
		
		Reducer<Text, Text, Text, Text>.Context context = getReducerContext(reducer,dwr,conf,taskId); 
			

		Iterable<Text> input = new Iterable<Text>() {

			@Override
			public Iterator<Text> iterator() {
				List<Text> list = new ArrayList<Text>();

				list.add(new Text("http://foobar.com"));
				list.add(new Text("http://foobar.com"));
				list.add(new Text("http://goobar.com"));
				list.add(new Text("http://goobar.com"));
				list.add(new Text("http://bazbar.com"));
				list.add(new Text("http://bazbar.com"));
				list.add(new Text("http://cowbar.com"));
				list.add(new Text("http://cowbar.com"));

				return list.iterator();
			}

		};

		Text key = new Text("http://somebar.com");

		reducer.reduce(key, input, context);

		Map<Text, Text> map = dwr.getMap();

		assertNotNull(map);
		assertTrue(map.size() == 1);
		assertNotNull(map.get(new Text("http://somebar.com")));
		Text value = map.get(key);
		String parts[] = value.toString().split("\t");
		assertEquals(4,parts.length);
		
		String last = "";
		for(String part: parts) {
			String fields[] = part.split(ItemToItemGrouping.INTERNAL_DELIMITER);
			
			assertEquals(2,fields.length);
			assertFalse(last.equals(fields[0]));
			last=  fields[0];
			assertEquals("0.25",fields[1]);
			
		}

		
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

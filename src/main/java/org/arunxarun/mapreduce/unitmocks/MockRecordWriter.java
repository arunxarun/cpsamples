package org.arunxarun.mapreduce.unitmocks;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * @author arun.x.arun@gmail.com 
 * mock of the RecordWriter class used in MapReduce -- writes values as they come in 
 * 
 */
public class MockRecordWriter<K, V> extends RecordWriter<K, V> {

	private Map<K, V> map;
	private Map<K,List<V>> mapOfLists;
	
	public MockRecordWriter() {
		map = new HashMap<K, V>();
		mapOfLists = new HashMap<K,List<V>>();
	}

	@Override
	public void close(TaskAttemptContext arg0) throws IOException,
			InterruptedException {

	}

	@Override
	public void write(K arg0, V arg1) throws IOException, InterruptedException {
		List<V> list = mapOfLists.get(arg0);
		if(list == null) {
			list = new ArrayList<V>();
			mapOfLists.put(arg0,list);
		}
		list.add(arg1);
		
		map.put(arg0, arg1);
		
	}

	public Map<K, V> getMap() {
		return map;
	}
	
	public Map<K,List<V>> getMapOfLists() {
		return mapOfLists;
	}

}

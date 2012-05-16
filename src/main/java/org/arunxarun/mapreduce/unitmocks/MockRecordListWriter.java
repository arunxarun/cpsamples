package org.arunxarun.mapreduce.unitmocks;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class MockRecordListWriter<K,V> extends RecordWriter<K,V> {

	private Map<K,List<V>> map;
	
	
	public MockRecordListWriter() {
		map = new HashMap<K,List<V>>();
    }
	
	
	@Override
    public void close(TaskAttemptContext arg0) throws IOException, InterruptedException {
	    	    
    }

	@Override
    public void write(K arg0, V arg1) throws IOException, InterruptedException {
		
		List<V> values = map.get(arg0);
		
		if(values == null) {
			values = new ArrayList<V>();
			map.put(arg0,values);
		}
		
		values.add(arg1);
	    
	    
    }
	
	public Map<K,List<V>> getMap() {
		return map;
	}

}

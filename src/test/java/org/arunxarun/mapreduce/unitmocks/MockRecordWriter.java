package org.arunxarun.mapreduce.unitmocks;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class MockRecordWriter<K,V> extends RecordWriter<K,V> {

	private Map<K,V> map;
	
	
	public MockRecordWriter() {
		map = new HashMap<K,V>();
    }
	
	
	@Override
    public void close(TaskAttemptContext arg0) throws IOException, InterruptedException {
	    
    }

	@Override
    public void write(K arg0, V arg1) throws IOException, InterruptedException {
	    map.put(arg0, arg1);
	    
    }
	
	public Map<K,V> getMap() {
		return map;
	}

}

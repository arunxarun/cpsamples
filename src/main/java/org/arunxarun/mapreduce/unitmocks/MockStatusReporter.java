package org.arunxarun.mapreduce.unitmocks;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.StatusReporter;

public class MockStatusReporter extends StatusReporter {

	Map<Enum<?>,Counter> enumDCMap = new HashMap<Enum<?>,Counter>();
	Map<String,Counter> groupNameDCMap = new HashMap<String,Counter>();
	
	@Override
	public Counter getCounter(Enum<?> enumName) {
		
		Counter counter =  enumDCMap.get(enumName);
		if(counter == null) {
			counter = new MockCounter(enumName);
			enumDCMap.put(enumName, counter);
		}
		
		return counter;
	}

	@Override
	public Counter getCounter(String group, String name) {
		String key = generateKey(group,name);
		Counter counter = groupNameDCMap.get(key);
		
		if(counter == null) {
			counter = new MockCounter(group,name);
			groupNameDCMap.put(key, counter);
		}
		
		return counter;
		
	}

	private String generateKey(String group, String name) {
		
		return group+":"+name;
	}

	@Override
	public void progress() {
		// TODO Auto-generated method stub

	}

	@Override
	public void setStatus(String arg0) {
		// TODO Auto-generated method stub

	}

}

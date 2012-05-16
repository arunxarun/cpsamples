package org.arunxarun.mapreduce.unitmocks;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;

/**
 * @author arun.x.arun@gmail.com 
 * mock of the InputSplit class used in MapReduce
 * 
 */
public class MockInputSplit extends InputSplit {

	@Override
	public long getLength() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String[] getLocations() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return null;
	}

}

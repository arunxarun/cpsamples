package org.arunxarun.mapreduce.unitmocks;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;

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

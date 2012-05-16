package org.arunxarun.mapreduce.unitmocks;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.StatusReporter;

public class MockRawComparator extends StatusReporter {

	@Override
	public Counter getCounter(Enum<?> arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Counter getCounter(String arg0, String arg1) {
		// TODO Auto-generated method stub
		return null;
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

package org.arunxarun.mapreduce.unitmocks;

import java.io.IOException;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.util.Progress;

/**
 * @author arun.x.arun@gmail.com 
 * mock of the RawKeyValueIterator class used in MapReduce
 * 
 */
public class MockRawKeyValueIterator implements RawKeyValueIterator {

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public DataInputBuffer getKey() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Progress getProgress() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DataInputBuffer getValue() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean next() throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

}

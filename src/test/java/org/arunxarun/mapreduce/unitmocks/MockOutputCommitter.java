package org.arunxarun.mapreduce.unitmocks;

import java.io.IOException;

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class MockOutputCommitter extends OutputCommitter {

	@Override
	public void abortTask(TaskAttemptContext arg0) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void cleanupJob(JobContext arg0) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void commitTask(TaskAttemptContext arg0) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean needsTaskCommit(TaskAttemptContext arg0) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void setupJob(JobContext arg0) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setupTask(TaskAttemptContext arg0) throws IOException {
		// TODO Auto-generated method stub

	}

}

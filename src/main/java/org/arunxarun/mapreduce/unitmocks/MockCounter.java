package org.arunxarun.mapreduce.unitmocks;

import org.apache.hadoop.mapreduce.Counter;

/**
 * @author arun.x.arun@gmail.com 
 * mock of the Counter class used in MapReduce
 * 
 */
public class MockCounter extends Counter {

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 0;
		result = prime * result + (int) (count ^ (count >>> 32));
		result = prime * result
				+ ((enumName == null) ? 0 : enumName.hashCode());
		result = prime * result + ((group == null) ? 0 : group.hashCode());
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		MockCounter other = (MockCounter) obj;
		if (count != other.count)
			return false;
		if (enumName == null) {
			if (other.enumName != null)
				return false;
		} else if (!enumName.equals(other.enumName))
			return false;
		if (group == null) {
			if (other.group != null)
				return false;
		} else if (!group.equals(other.group))
			return false;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		return true;
	}

	private String name;
	private String group;
	private Enum<?> enumName;
	private long count;

	public MockCounter() {

	}

	public MockCounter(Enum<?> enumName) {
		this.enumName = enumName;
	}

	public MockCounter(String group, String name) {
		this.group = group;
		this.name = name;
	}

	@Override
	public void increment(long incr) {
		count++;
	}

	@Override
	public long getValue() {
		return count;
	}

}

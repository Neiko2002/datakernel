/*
 * Copyright (C) 2015 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.datakernel.jmx.helper;

import io.datakernel.jmx.JmxStats;

import javax.management.openmbean.SimpleType;
import java.util.SortedMap;
import java.util.TreeMap;

public final class JmxStatsStub implements JmxStats<JmxStatsStub> {

	private long sum = 0L;
	private int count = 0;

	private long lastTimestamp = 0L;
	private double lastSmoothingWindow = 0.0;
	private int refreshStatsInvocations = 0;

	public void recordValue(long value) {
		sum += value;
		++count;
	}

	public long getSum() {
		return sum;
	}

	public int getCount() {
		return count;
	}

	public long getLastTimestamp() {
		return lastTimestamp;
	}

	public double getLastSmoothingWindow() {
		return lastSmoothingWindow;
	}

	public int getRefreshStatsInvocations() {
		return refreshStatsInvocations;
	}

	@Override
	public void add(JmxStatsStub stats) {
		this.sum += stats.sum;
		this.count += stats.count;
	}

	@Override
	public void refreshStats(long timestamp, double smoothingWindow) {
		lastTimestamp = timestamp;
		lastSmoothingWindow = smoothingWindow;
		++refreshStatsInvocations;
	}

	@Override
	public SortedMap<String, TypeAndValue> getAttributes() {
		SortedMap<String, TypeAndValue> attributes = new TreeMap<>();
		attributes.put("sum", new TypeAndValue(SimpleType.LONG, sum));
		attributes.put("count", new TypeAndValue(SimpleType.INTEGER, count));
		return attributes;
	}
}
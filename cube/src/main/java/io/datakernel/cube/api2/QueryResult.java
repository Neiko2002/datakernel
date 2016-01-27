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

package io.datakernel.cube.api2;

import io.datakernel.cube.api.TotalsPlaceholder;

import java.util.List;

public final class QueryResult {
	private final List<Object> records;
	private final Class recordClass;
	private final TotalsPlaceholder totals;

	private final List<String> dimensions;
	private final List<String> attributes;
	private final List<String> measures;

	public static QueryResult emptyResult() {
		return new QueryResult(null, null, null, null, null, null);
	}

	public QueryResult(List<Object> records, Class recordClass, List<String> dimensions, List<String> attributes,
	                   List<String> measures, TotalsPlaceholder totals) {
		this.records = records;
		this.recordClass = recordClass;
		this.dimensions = dimensions;
		this.attributes = attributes;
		this.measures = measures;
		this.totals = totals;
	}

	public boolean isEmpty() {
		return records == null;
	}

	public List<Object> getRecords() {
		return records;
	}

	public Class getRecordClass() {
		return recordClass;
	}

	public List<String> getDimensions() {
		return dimensions;
	}

	public List<String> getAttributes() {
		return attributes;
	}

	public List<String> getMeasures() {
		return measures;
	}

	public TotalsPlaceholder getTotals() {
		return totals;
	}
}

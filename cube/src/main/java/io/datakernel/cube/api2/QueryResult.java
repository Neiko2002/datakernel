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

import com.google.common.base.MoreObjects;
import io.datakernel.cube.api.TotalsPlaceholder;

import java.util.List;
import java.util.Set;

public final class QueryResult {
	private final List<Object> records;
	private final Class recordClass;

	private final TotalsPlaceholder totals;

	private final Set<List<String>> drillDowns;
	private final List<String> dimensions;
	private final List<String> attributes;
	private final List<String> measures;

	private final Object filterAttributesPlaceholder;
	private final List<String> filterAttributes;
	private final Class filterAttributesClass;

	public QueryResult(List<Object> records, Class recordClass, TotalsPlaceholder totals,
	                   Set<List<String>> drillDowns, List<String> dimensions, List<String> attributes,
	                   List<String> measures, Object filterAttributesPlaceholder, List<String> filterAttributes,
	                   Class filterAttributesClass) {
		this.records = records;
		this.recordClass = recordClass;
		this.totals = totals;
		this.drillDowns = drillDowns;
		this.dimensions = dimensions;
		this.attributes = attributes;
		this.measures = measures;
		this.filterAttributesPlaceholder = filterAttributesPlaceholder;
		this.filterAttributes = filterAttributes;
		this.filterAttributesClass = filterAttributesClass;
	}

	public List<Object> getRecords() {
		return records;
	}

	public Class getRecordClass() {
		return recordClass;
	}

	public TotalsPlaceholder getTotals() {
		return totals;
	}

	public Set<List<String>> getDrillDowns() {
		return drillDowns;
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

	public Object getFilterAttributesPlaceholder() {
		return filterAttributesPlaceholder;
	}

	public List<String> getFilterAttributes() {
		return filterAttributes;
	}

	public Class getFilterAttributesClass() {
		return filterAttributesClass;
	}

	@Override
	public String toString() {
		return MoreObjects.toStringHelper(this)
				.add("records", records)
				.add("recordClass", recordClass)
				.add("totals", totals)
				.add("dimensions", dimensions)
				.add("attributes", attributes)
				.add("measures", measures)
				.add("filterAttributesPlaceholder", filterAttributesPlaceholder)
				.add("filterAttributes", filterAttributes)
				.add("filterAttributesClass", filterAttributesClass)
				.toString();
	}
}

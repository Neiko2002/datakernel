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

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.datakernel.aggregation_db.AggregationQuery;
import io.datakernel.aggregation_db.api.QueryException;
import io.datakernel.cube.api.ReportingQuery;
import io.datakernel.http.HttpRequest;

import java.util.List;
import java.util.Set;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static io.datakernel.cube.api.CommonUtils.getListOfStrings;
import static io.datakernel.cube.api.CommonUtils.getSetOfStrings;

public final class HttpRequestProcessor implements RequestProcessor<HttpRequest> {
	public static final String DIMENSIONS_PARAM = "dimensions";
	public static final String MEASURES_PARAM = "measures";
	public static final String ATTRIBUTES_PARAM = "attributes";
	public static final String FILTERS_PARAM = "filters";
	public static final String SORT_PARAM = "sort";
	public static final String LIMIT_PARAM = "limit";
	public static final String OFFSET_PARAM = "offset";
	public static final String SEARCH_PARAM = "search";
	public static final String FIELDS_PARAM = "fields";
	public static final String METADATA_FIELDS_PARAM = "metadata";

	private final Gson gson;

	public HttpRequestProcessor(Gson gson) {
		this.gson = gson;
	}

	@Override
	public ReportingQuery apply(HttpRequest request) {
		List<String> dimensions = parseListOfStrings(request.getParameter(DIMENSIONS_PARAM));
		List<String> measures = parseMeasures(request.getParameter(MEASURES_PARAM));
		List<String> attributes = parseListOfStrings(request.getParameter(ATTRIBUTES_PARAM));
		AggregationQuery.QueryPredicates predicates = parsePredicates(request.getParameter(FILTERS_PARAM));
		List<AggregationQuery.QueryOrdering> ordering = parseOrdering(request.getParameter(SORT_PARAM));
		Integer limit = valueOrNull(request.getParameter(LIMIT_PARAM));
		Integer offset = valueOrNull(request.getParameter(OFFSET_PARAM));
		String searchString = request.getParameter(SEARCH_PARAM);
		Set<String> fields = getSetOfStrings(gson, request.getParameter(FIELDS_PARAM));
		Set<String> metadataFields = getSetOfStrings(gson, request.getParameter(METADATA_FIELDS_PARAM));

		if (dimensions.isEmpty() && attributes.isEmpty())
			throw new QueryException("At least one dimension or attribute must be specified");

		return new ReportingQuery(dimensions, measures, attributes, predicates, ordering, limit, offset, searchString,
				fields, metadataFields);
	}

	private AggregationQuery.QueryPredicates parsePredicates(String json) {
		AggregationQuery.QueryPredicates predicates = null;

		if (json != null) {
			predicates = gson.fromJson(json, AggregationQuery.QueryPredicates.class);
		}

		return predicates == null ? new AggregationQuery.QueryPredicates() : predicates;
	}

	private List<AggregationQuery.QueryOrdering> parseOrdering(String json) {
		if (json == null)
			return null;

		return gson.fromJson(json, new TypeToken<List<AggregationQuery.QueryOrdering>>() {}.getType());
	}

	private List<String> parseMeasures(String json) {
		List<String> measures = parseListOfStrings(json);

		if (measures.isEmpty())
			throw new QueryException("Measures must be specified");

		return measures;
	}

	private List<String> parseListOfStrings(String json) {
		if (json == null)
			return newArrayList();

		return getListOfStrings(gson, json);
	}

	private static Integer valueOrNull(String str) {
		if (str == null)
			return null;
		return str.isEmpty() ? null : Integer.valueOf(str);
	}
}

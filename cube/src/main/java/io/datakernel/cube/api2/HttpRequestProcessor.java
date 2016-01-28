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
import io.datakernel.aggregation_db.AggregationQuery;
import io.datakernel.aggregation_db.api.QueryException;
import io.datakernel.cube.api.ReportingQuery;
import io.datakernel.http.HttpRequest;

import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static io.datakernel.cube.api.CommonUtils.getListOfStrings;

public final class HttpRequestProcessor implements RequestProcessor<HttpRequest> {
	private final Gson gson;

	public HttpRequestProcessor(Gson gson) {
		this.gson = gson;
	}

	@Override
	public ReportingQuery apply(HttpRequest request) {
		List<String> dimensions = parseListOfStrings(request.getParameter("dimensions"));
		List<String> measures = parseMeasures(request.getParameter("measures"));
		List<String> attributes = parseListOfStrings(request.getParameter("attributes"));
		AggregationQuery.QueryPredicates predicates = parsePredicates(request.getParameter("filters"));
		AggregationQuery.QueryOrdering ordering = parseOrdering(request.getParameter("sort"));
		Integer limit = valueOrNull(request.getParameter("limit"));
		Integer offset = valueOrNull(request.getParameter("offset"));
		boolean ignoreMeasures = getBoolean(request.getParameter("ignore-measures"));

		if (dimensions.isEmpty() && attributes.isEmpty())
			throw new QueryException("At least one dimension or attribute must be specified");

		return new ReportingQuery(dimensions, measures, attributes, predicates, ordering, limit, offset, ignoreMeasures);
	}

	private AggregationQuery.QueryPredicates parsePredicates(String json) {
		AggregationQuery.QueryPredicates predicates = null;

		if (json != null) {
			predicates = gson.fromJson(json, AggregationQuery.QueryPredicates.class);
		}

		return predicates == null ? new AggregationQuery.QueryPredicates() : predicates;
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

	private AggregationQuery.QueryOrdering parseOrdering(String json) {
		List<String> ordering = parseListOfStrings(json);

		if (ordering.isEmpty())
			return null;

		if (ordering.size() != 2)
			throw new QueryException("Incorrect 'sort' parameter format");

		String field = ordering.get(0);
		String direction = ordering.get(1);

		if (direction.equals("asc"))
			return AggregationQuery.QueryOrdering.asc(field);

		if (direction.equals("desc"))
			return AggregationQuery.QueryOrdering.desc(field);

		throw new QueryException("Incorrect ordering specified in 'sort' parameter");
	}

	private static Integer valueOrNull(String str) {
		if (str == null)
			return null;
		return str.isEmpty() ? null : Integer.valueOf(str);
	}

	private static boolean getBoolean(String str) {
		return str != null && (str.equals("1") || str.equals("true"));
	}
}

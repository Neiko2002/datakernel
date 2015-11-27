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

package io.datakernel.cube.api;

import com.google.gson.*;
import com.google.gson.reflect.TypeToken;
import io.datakernel.aggregation_db.AggregationQuery;
import io.datakernel.aggregation_db.AggregationStructure;
import io.datakernel.aggregation_db.api.QueryException;
import io.datakernel.aggregation_db.fieldtype.FieldType;
import io.datakernel.aggregation_db.keytype.KeyType;
import io.datakernel.async.ResultCallback;
import io.datakernel.codegen.AsmBuilder;
import io.datakernel.codegen.ExpressionComparatorNullable;
import io.datakernel.codegen.ExpressionSequence;
import io.datakernel.codegen.utils.DefiningClassLoader;
import io.datakernel.cube.Cube;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.server.AsyncHttpServlet;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducer;
import io.datakernel.util.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.*;

import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Maps.newLinkedHashMap;
import static com.google.common.collect.Sets.newHashSet;
import static io.datakernel.codegen.Expressions.*;
import static io.datakernel.cube.api.CommonUtils.*;
import static io.datakernel.util.ByteBufStrings.decodeUTF8;

public final class ReportingQueryHandler implements AsyncHttpServlet {
	private static final Logger logger = LoggerFactory.getLogger(ReportingQueryHandler.class);

	private final Gson gson;
	private final Cube cube;
	private final AggregationStructure structure;
	private final ReportingConfiguration reportingConfiguration;
	private final NioEventloop eventloop;
	private final DefiningClassLoader classLoader;
	private final Resolver resolver;
	private final JsonParser parser = new JsonParser();

	public ReportingQueryHandler(Gson gson, Cube cube, NioEventloop eventloop, DefiningClassLoader classLoader) {
		this.gson = gson;
		this.cube = cube;
		this.structure = cube.getStructure();
		this.reportingConfiguration = cube.getReportingConfiguration();
		this.eventloop = eventloop;
		this.classLoader = classLoader;
		this.resolver = new Resolver(classLoader, cube.getResolvers());
	}

	private static class ResultsCombiner implements ResultCallback<QueryJsonResult> {
		private final int queries;
		private final TreeMap<Integer, JsonObject> queryResults = new TreeMap<>();
		private final List<Map<String, JsonElement>> request;
		private final ResultCallback<HttpResponse> callback;
		private boolean errorReceived;
		private Stopwatch sw = Stopwatch.createStarted();

		public ResultsCombiner(int queries, List<Map<String, JsonElement>> request, ResultCallback<HttpResponse> callback) {
			this.queries = queries;
			this.request = request;
			this.callback = callback;
		}

		@Override
		public void onResult(QueryJsonResult result) {
			if (errorReceived)
				return;

			queryResults.put(result.queryId, result.jsonResult);
			if (queryResults.size() == queries) {
				sendResult();
				logger.info("Sent response to request {} [time={}]", request, sw);
			}
		}

		@Override
		public void onException(Exception exception) {
			if (errorReceived)
				return;

			callback.onResult(exception.getMessage() == null ? response500("Unknown server error") : response500(exception.getMessage()));
			logger.info("Sent error response to request {} [time={}]", request, sw);
			errorReceived = true;
		}

		private void sendResult() {
			if (queryResults.size() == 1) {
				callback.onResult(createResponse(queryResults.get(0).toString()));
				return;
			}

			JsonArray array = new JsonArray();
			for (JsonObject jsonObject : queryResults.values()) {
				array.add(jsonObject);
			}

			callback.onResult(createResponse(array.toString()));
		}
	}

	@Override
	public void serveAsync(HttpRequest httpRequest, final ResultCallback<HttpResponse> callback) {
		String requestBody = decodeUTF8(httpRequest.getBody());
		try {
			List<Map<String, JsonElement>> requests = parseRequest(requestBody);
			ResultsCombiner resultsCombiner = new ResultsCombiner(requests.size(), requests, callback);
			for (int i = 0; i < requests.size(); ++i) {
				new ReportingRequestProcessor().processRequest(requests.get(i), i, resultsCombiner);
			}
		} catch (QueryException e) {
			logger.info("Request [{}] could not be processed because of error: {}", requestBody, e.getMessage());
			callback.onResult(response500(e.getMessage()));
		} catch (JsonParseException e) {
			logger.info("Failed to parse JSON in request [{}]", requestBody);
			callback.onResult(response500("Failed to parse JSON"));
		} catch (RuntimeException e) {
			logger.error("Unknown exception occurred while processing request [{}]", requestBody, e);
			callback.onResult(response500("Unknown server error"));
		}
	}

	@SuppressWarnings("unchecked")
	private List<Map<String, JsonElement>> parseRequest(String requestJson) {
		JsonElement json = parser.parse(requestJson);

		if (json.isJsonArray()) {
			Type type = new TypeToken<List<Map<String, JsonElement>>>() {}.getType();
			return gson.fromJson(requestJson, type);
		}

		if (json.isJsonObject()) {
			Type type = new TypeToken<Map<String, JsonElement>>() {}.getType();
			Map<String, JsonElement> map = gson.fromJson(requestJson, type);
			return newArrayList(map);
		}

		throw new QueryException("Incorrect request format: should be either object or array");
	}

	private final static class QueryJsonResult {
		private final int queryId;
		private final JsonObject jsonResult;

		public QueryJsonResult(int queryId, JsonObject jsonResult) {
			this.queryId = queryId;
			this.jsonResult = jsonResult;
		}
	}

	private class ReportingRequestProcessor {
		private Map<String, JsonElement> request;
		private int queryId;
		private ResultCallback<QueryJsonResult> callback;

		private Map<AttributeResolver, List<String>> resolverKeys = newLinkedHashMap();
		private Map<String, Class<?>> attributeTypes = newLinkedHashMap();
		private Map<String, Object> keyConstants = newHashMap();

		private List<String> requestDimensions = newArrayList();
		private Set<String> storedDimensions = newHashSet();

		private AggregationQuery query;

		private Map<String, AggregationQuery.QueryPredicate> predicates;

		private List<String> queryMeasures;
		private Set<String> storedMeasures = newHashSet();
		private Set<String> computedMeasures = newHashSet();

		private List<String> attributes = newArrayList();

		private List<String> ordering = newArrayList();

		private boolean additionalSortingRequired;
		private String orderingField;
		private boolean ascendingOrdering;

		private Class<QueryResultPlaceholder> resultClass;
		private Comparator<QueryResultPlaceholder> comparator;
		private Integer limit;
		private Integer offset;

		private void processRequest(final Map<String, JsonElement> request, final int queryId,
		                            final ResultCallback<QueryJsonResult> callback) {
			try {
				this.request = request;
				this.queryId = queryId;
				this.callback = callback;
				doProcess();
			} catch (QueryException e) {
				logger.info("Request {} could not be processed because of error: {}", request, e.getMessage());
				callback.onException(e);
			} catch (JsonParseException e) {
				logger.info("Failed to parse JSON in request {}", request);
				callback.onException(new RuntimeException("Failed to parse JSON"));
			} catch (RuntimeException e) {
				logger.error("Unknown exception occurred while processing request {}", request, e);
				callback.onException(new RuntimeException());
			}
		}

		private void doProcess() {
			final Stopwatch sw = Stopwatch.createStarted();
			processPredicates(request.get("filters"));

			parseAttributes(request.get("attributes"));
			processAttributes();

			parseDimensions(request.get("dimensions"));

			if (requestDimensions.isEmpty() && attributes.isEmpty())
				throw new QueryException("At least one dimension or attribute must be specified");

			processDimensions();

			parseMeasures(request.get("measures"));
			processMeasures();

			query
					.keys(newArrayList(storedDimensions))
					.fields(newArrayList(storedMeasures));

			parseOrdering(request.get("sort"));
			processOrdering();

			resultClass = createResultClass();
			StreamConsumers.ToList<QueryResultPlaceholder> consumerStream = queryCube();
			limit = valueOrNull(request.get("limit"), "limit");
			offset = valueOrNull(request.get("offset"), "offset");

			comparator = additionalSortingRequired ? generateComparator(orderingField, ascendingOrdering, resultClass) : null;
			consumerStream.setResultCallback(new ResultCallback<List<QueryResultPlaceholder>>() {
				@Override
				public void onResult(List<QueryResultPlaceholder> results) {
					try {
						processResults(results, sw);
					} catch (Exception e) {
						logger.error("Unknown exception occurred while processing results {}", e);
						callback.onException(new RuntimeException());
					}
				}

				@Override
				public void onException(Exception e) {
					logger.error("Executing query {} failed.", query, e);
					callback.onException(new RuntimeException());
				}
			});
		}

		private void processPredicates(JsonElement predicatesJson) {
			query = new AggregationQuery();
			AggregationQuery.QueryPredicates queryPredicates = addPredicatesToQuery(predicatesJson);
			predicates = queryPredicates == null ? null : queryPredicates.asMap();
		}

		private void parseAttributes(JsonElement attributesJson) {
			if (attributesJson == null)
				return;

			attributes = getListOfStrings(gson, attributesJson);
		}

		private void processAttributes() {
			for (String attribute : attributes) {
				AttributeResolver resolver = reportingConfiguration.getAttributeResolver(attribute);
				if (resolver == null)
					throw new QueryException("Cube does not contain resolver for '" + attribute + "'");

				List<String> key = reportingConfiguration.getKeyForResolver(resolver);

				boolean usingStoredDimension = false;
				for (String keyComponent : key) {
					if (predicates != null && predicates.get(keyComponent) instanceof AggregationQuery.QueryPredicateEq) {
						if (usingStoredDimension)
							throw new QueryException("Incorrect filter: using 'equals' predicate when prefix of this compound key is not fully defined");
						else
							keyConstants.put(keyComponent, ((AggregationQuery.QueryPredicateEq) predicates.get(keyComponent)).value);
					} else {
						storedDimensions.add(keyComponent);
						usingStoredDimension = true;
					}
				}

				resolverKeys.put(resolver, key);
				attributeTypes.put(attribute, reportingConfiguration.getAttributeType(attribute));
			}
		}

		private AggregationQuery.QueryPredicates addPredicatesToQuery(JsonElement predicatesJson) {
			AggregationQuery.QueryPredicates queryPredicates = null;

			if (predicatesJson != null) {
				queryPredicates = gson.fromJson(predicatesJson, AggregationQuery.QueryPredicates.class);
			}

			if (queryPredicates != null) {
				query.predicates(queryPredicates);
			}

			return queryPredicates;
		}

		private void parseDimensions(JsonElement dimensionsJson) {
			if (dimensionsJson == null)
				return;

			requestDimensions = getListOfStrings(gson, dimensionsJson);
		}

		private void processDimensions() {
			for (String dimension : requestDimensions) {
				if (structure.containsKey(dimension))
					storedDimensions.add(dimension);
				else
					throw new QueryException("Cube does not contain dimension with name '" + dimension + "'");
			}
		}

		private void parseMeasures(JsonElement measuresJson) {
			queryMeasures = getListOfStrings(gson, measuresJson);
			if (queryMeasures == null || queryMeasures.isEmpty())
				throw new QueryException("Measures must be specified");
		}

		private void processMeasures() {
			for (String queryMeasure : queryMeasures) {
				if (structure.containsOutputField(queryMeasure)) {
					storedMeasures.add(queryMeasure);
				} else if (reportingConfiguration.containsComputedMeasure(queryMeasure)) {
					ReportingDSLExpression reportingDSLExpression = reportingConfiguration.getExpressionForMeasure(queryMeasure);
					storedMeasures.addAll(reportingDSLExpression.getMeasureDependencies());
					computedMeasures.add(queryMeasure);
				} else {
					throw new QueryException("Cube does not contain measure with name '" + queryMeasure + "'");
				}
			}
		}

		private void parseOrdering(JsonElement orderingsJson) {
			if (orderingsJson == null)
				return;

			ordering = getListOfStrings(gson, orderingsJson);
		}

		private void processOrdering() {
			if (ordering.isEmpty())
				return;

			if (ordering.size() != 2)
				throw new QueryException("Incorrect 'sort' parameter format");

			orderingField = ordering.get(0);
			additionalSortingRequired = computedMeasures.contains(orderingField) || attributeTypes.containsKey(orderingField);

			if (!storedDimensions.contains(orderingField) && !storedMeasures.contains(orderingField) && !additionalSortingRequired) {
				throw new QueryException("Ordering is specified by not requested field");
			}

			String direction = ordering.get(1);

			if (direction.equals("asc"))
				ascendingOrdering = true;
			else if (direction.equals("desc"))
				ascendingOrdering = false;
			else {
				throw new QueryException("Incorrect ordering specified in 'sort' parameter");
			}

			if (!additionalSortingRequired)
				addOrderingToQuery();
		}

		private void addOrderingToQuery() {
			if (ascendingOrdering)
				query.orderAsc(orderingField);
			else
				query.orderDesc(orderingField);
		}

		private Class<QueryResultPlaceholder> createResultClass() {
			AsmBuilder<QueryResultPlaceholder> builder = new AsmBuilder<>(classLoader, QueryResultPlaceholder.class);
			List<String> resultKeys = query.getResultKeys();
			List<String> resultFields = query.getResultFields();
			for (String key : resultKeys) {
				KeyType keyType = structure.getKeyType(key);
				builder.field(key, keyType.getDataType());
			}
			for (String field : resultFields) {
				FieldType fieldType = structure.getOutputFieldType(field);
				builder.field(field, fieldType.getDataType());
			}
			for (Map.Entry<String, Class<?>> nameEntry : attributeTypes.entrySet()) {
				builder.field(nameEntry.getKey(), nameEntry.getValue());
			}
			ExpressionSequence computeSequence = sequence();
			for (String computedMeasure : computedMeasures) {
				builder.field(computedMeasure, double.class);
				computeSequence.add(set(getter(self(), computedMeasure), reportingConfiguration.getComputedMeasureExpression(computedMeasure)));
			}
			builder.method("computeMeasures", computeSequence);
			return builder.defineClass();
		}

		@SuppressWarnings("unchecked")
		private StreamConsumers.ToList<QueryResultPlaceholder> queryCube() {
			StreamConsumers.ToList<QueryResultPlaceholder> consumerStream = StreamConsumers.toList(eventloop);
			StreamProducer<QueryResultPlaceholder> queryResultProducer = cube.query(resultClass, query);
			queryResultProducer.streamTo(consumerStream);
			return consumerStream;
		}

		@SuppressWarnings("unchecked")
		private void processResults(List<QueryResultPlaceholder> results, Stopwatch sw) {
			if (results.isEmpty()) {
				logger.info("Executed request {} (query {}) [time={}]", request, query, sw);
				callback.onResult(new QueryJsonResult(queryId, constructEmptyResult()));
				return;
			}

			TotalsPlaceholder totalsPlaceholder = computeTotals(results);
			computeMeasures(results);
			resolver.resolve((List) results, resultClass, attributeTypes, resolverKeys, keyConstants);
			sort(results);

			JsonObject jsonResult = constructQueryResultJson(results, resultClass,
					newArrayList(concat(requestDimensions, attributes)), queryMeasures, totalsPlaceholder);
			logger.info("Executed request {} (query {}) [time={}]", request, query, sw);
			callback.onResult(new QueryJsonResult(queryId, jsonResult));
		}

		private TotalsPlaceholder computeTotals(List<QueryResultPlaceholder> results) {
			TotalsPlaceholder totalsPlaceholder = createTotalsPlaceholder(resultClass, storedMeasures, computedMeasures);
			for (QueryResultPlaceholder record : results) {
				totalsPlaceholder.accumulate(record);
			}
			totalsPlaceholder.computeMeasures();
			return totalsPlaceholder;
		}

		private void computeMeasures(List<QueryResultPlaceholder> results) {
			for (QueryResultPlaceholder queryResult : results) {
				queryResult.computeMeasures();
			}
		}

		private void sort(List<QueryResultPlaceholder> results) {
			if (comparator != null) {
				Collections.sort(results, comparator);
			}
		}

		private JsonObject constructEmptyResult() {
			JsonObject jsonResult = new JsonObject();
			jsonResult.add("records", new JsonArray());
			jsonResult.add("totals", new JsonObject());
			jsonResult.addProperty("count", 0);
			return jsonResult;
		}

		private <T> JsonObject constructQueryResultJson(List<T> results, Class<?> resultClass,
		                                                List<String> resultKeys, List<String> resultFields,
		                                                TotalsPlaceholder totalsPlaceholder) {
			JsonObject jsonResult = new JsonObject();
			JsonArray jsonRecords = new JsonArray();
			JsonObject jsonTotals = new JsonObject();

			int start = offset == null ? 0 : offset;
			int end;

			if (limit == null)
				end = results.size();
			else if (start + limit > results.size())
				end = results.size();
			else
				end = start + limit;

			FieldGetter[] fieldGetters = new FieldGetter[resultFields.size()];
			for (int i = 0; i < resultFields.size(); i++) {
				String field = resultFields.get(i);
				fieldGetters[i] = generateGetter(classLoader, resultClass, field);
			}

			FieldGetter[] keyGetters = new FieldGetter[resultKeys.size()];
			KeyType[] keyTypes = new KeyType[resultKeys.size()];
			for (int i = 0; i < resultKeys.size(); i++) {
				String key = resultKeys.get(i);
				keyGetters[i] = generateGetter(classLoader, resultClass, key);
				keyTypes[i] = structure.getKeyType(key);
			}

			for (int i = start; i < end; ++i) {
				T result = results.get(i);
				JsonObject resultJsonObject = new JsonObject();

				for (int j = 0; j < resultKeys.size(); j++) {
					Object value = keyGetters[j].get(result);
					JsonElement json;
					if (keyTypes[j] == null)
						json = value == null ? null : new JsonPrimitive(value.toString());
					else
						json = keyTypes[j].toJson(value);
					resultJsonObject.add(resultKeys.get(j), json);
				}

				for (int j = 0; j < resultFields.size(); j++) {
					resultJsonObject.add(resultFields.get(j), new JsonPrimitive((Number) fieldGetters[j].get(result)));
				}

				jsonRecords.add(resultJsonObject);
			}

			for (String field : resultFields) {
				Object totalFieldValue = generateGetter(classLoader, totalsPlaceholder.getClass(), field).get(totalsPlaceholder);
				jsonTotals.add(field, new JsonPrimitive((Number) totalFieldValue));
			}

			jsonResult.add("records", jsonRecords);
			jsonResult.add("totals", jsonTotals);
			jsonResult.addProperty("count", results.size());

			return jsonResult;
		}

		private Integer valueOrNull(JsonElement json, String fieldName) {
			if (json == null)
				return null;

			if (json.isJsonPrimitive() && ((JsonPrimitive) json).isNumber()) {
				return json.getAsInt();
			}

			throw new QueryException("Incorrect " + fieldName + " format. Must be a number");
		}

		private TotalsPlaceholder createTotalsPlaceholder(Class<?> inputClass, Set<String> requestedStoredFields,
		                                                  Set<String> computedMeasureNames) {
			AsmBuilder<TotalsPlaceholder> builder = new AsmBuilder<>(classLoader, TotalsPlaceholder.class);

			for (String field : requestedStoredFields) {
				FieldType fieldType = structure.getOutputFieldType(field);
				builder.field(field, fieldType.getDataType());
			}
			for (String computedMeasure : computedMeasureNames) {
				builder.field(computedMeasure, double.class);
			}

			ExpressionSequence accumulateSequence = sequence();
			for (String field : requestedStoredFields) {
				accumulateSequence.add(set(
						getter(self(), field),
						add(
								getter(self(), field),
								getter(cast(arg(0), inputClass), field))));
			}
			builder.method("accumulate", accumulateSequence);

			ExpressionSequence computeSequence = sequence();
			for (String computedMeasure : computedMeasureNames) {
				computeSequence.add(set(getter(self(), computedMeasure), reportingConfiguration.getComputedMeasureExpression(computedMeasure)));
			}
			builder.method("computeMeasures", computeSequence);

			return builder.newInstance();
		}

		@SuppressWarnings("unchecked")
		private Comparator<QueryResultPlaceholder> generateComparator(String fieldName, boolean ascending,
		                                                              Class<QueryResultPlaceholder> fieldClass) {
			AsmBuilder<Comparator> builder = new AsmBuilder<>(classLoader, Comparator.class);
			ExpressionComparatorNullable comparator = comparatorNullable();
			if (ascending)
				comparator.add(
						getter(cast(arg(0), fieldClass), fieldName),
						getter(cast(arg(1), fieldClass), fieldName));
			else
				comparator.add(
						getter(cast(arg(1), fieldClass), fieldName),
						getter(cast(arg(0), fieldClass), fieldName));

			builder.method("compare", comparator);

			return builder.newInstance();
		}
	}
}

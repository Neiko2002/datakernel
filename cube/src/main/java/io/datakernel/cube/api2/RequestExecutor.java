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

import com.google.common.base.Predicate;
import com.google.common.collect.Maps;
import io.datakernel.aggregation_db.AggregationQuery;
import io.datakernel.aggregation_db.AggregationStructure;
import io.datakernel.aggregation_db.api.QueryException;
import io.datakernel.aggregation_db.fieldtype.FieldType;
import io.datakernel.aggregation_db.keytype.KeyType;
import io.datakernel.async.ResultCallback;
import io.datakernel.codegen.*;
import io.datakernel.codegen.utils.DefiningClassLoader;
import io.datakernel.cube.Cube;
import io.datakernel.cube.DrillDowns;
import io.datakernel.cube.api.*;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.google.common.base.Predicates.in;
import static com.google.common.collect.Iterables.*;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Maps.newLinkedHashMap;
import static com.google.common.collect.Sets.newHashSet;
import static io.datakernel.codegen.Expressions.*;
import static io.datakernel.cube.api.CommonUtils.instantiate;
import static java.util.Collections.singletonList;

public final class RequestExecutor {
	private static final Logger logger = LoggerFactory.getLogger(RequestExecutor.class);

	private final Cube cube;
	private final AggregationStructure structure;
	private final ReportingConfiguration reportingConfiguration;
	private final Eventloop eventloop;
	private final DefiningClassLoader classLoader;
	private final Resolver resolver;

	public RequestExecutor(Cube cube, AggregationStructure structure, ReportingConfiguration reportingConfiguration,
	                       Eventloop eventloop, DefiningClassLoader classLoader, Resolver resolver) {
		this.cube = cube;
		this.structure = structure;
		this.reportingConfiguration = reportingConfiguration;
		this.eventloop = eventloop;
		this.classLoader = classLoader;
		this.resolver = resolver;
	}

	public void execute(ReportingQuery query, ResultCallback<QueryResult> resultCallback) {
		new Context().execute(query, resultCallback);
	}

	class Context {
		Map<AttributeResolver, List<String>> resolverKeys = newLinkedHashMap();
		Map<String, Class<?>> attributeTypes = newLinkedHashMap();
		Map<String, Object> keyConstants = newHashMap();

		List<String> filterAttributes = newArrayList();
		Map<String, Class<?>> filterAttributeTypes = newLinkedHashMap();

		List<String> queryDimensions = newArrayList();
		Set<String> storedDimensions = newHashSet();

		DrillDowns drillDowns;

		AggregationQuery query = new AggregationQuery();

		AggregationQuery.QueryPredicates queryPredicates;
		Map<String, AggregationQuery.QueryPredicate> predicates;

		List<String> queryMeasures;
		Set<String> storedMeasures = newHashSet();
		Set<String> computedMeasures = newHashSet();
		boolean ignoreMeasures;

		List<String> attributes = newArrayList();

		AggregationQuery.QueryOrdering ordering;
		boolean additionalSortingRequired;

		Class<QueryResultPlaceholder> resultClass;
		Comparator<QueryResultPlaceholder> comparator;
		Integer limit;
		Integer offset;
		String searchString;

		void execute(ReportingQuery reportingQuery, final ResultCallback<QueryResult> resultCallback) {
			queryDimensions = reportingQuery.getDimensions();
			queryMeasures = reportingQuery.getMeasures();
			ignoreMeasures = reportingQuery.isIgnoreMeasures();
			queryPredicates = reportingQuery.getFilters();
			predicates = transformPredicates(queryPredicates);
			attributes = reportingQuery.getAttributes();
			ordering = reportingQuery.getSort();
			limit = reportingQuery.getLimit();
			offset = reportingQuery.getOffset();
			searchString = reportingQuery.getSearchString();

			processAttributes();
			processDimensions();
			processMeasures();
			processOrdering();

			query
					.keys(newArrayList(storedDimensions))
					.fields(newArrayList(storedMeasures))
					.predicates(newArrayList(predicates.values()));

			resultClass = createResultClass();
			StreamConsumers.ToList<QueryResultPlaceholder> consumerStream = queryCube();
			comparator = additionalSortingRequired ? generateComparator(ordering.getPropertyName(), ordering.isAsc(), resultClass) : null;

			consumerStream.setResultCallback(new ResultCallback<List<QueryResultPlaceholder>>() {
				@Override
				public void onResult(List<QueryResultPlaceholder> results) {
					try {
						processResults(results, resultCallback);
					} catch (Exception e) {
						logger.error("Unknown exception occurred while processing results {}", e);
						resultCallback.onException(e);
					}
				}

				@Override
				public void onException(Exception e) {
					logger.error("Executing query {} failed.", query, e);
					resultCallback.onException(e);
				}
			});
		}

		Map<String, AggregationQuery.QueryPredicate> transformPredicates(AggregationQuery.QueryPredicates predicates) {
			return predicates == null ? Maps.<String, AggregationQuery.QueryPredicate>newHashMap() : predicates.asMap();
		}

		void processDimensions() {
			List<String> dimensions = newArrayList();

			for (String dimension : queryDimensions) {
				if (structure.containsKey(dimension))
					dimensions.add(dimension);
				else
					throw new QueryException("Cube does not contain dimension with name '" + dimension + "'");
			}

			Set<String> usedDimensions = newHashSet();

			for (AggregationQuery.QueryPredicate predicate : predicates.values()) {
				usedDimensions.add(predicate.key);
			}

			for (String dimension : dimensions) {
				storedDimensions.addAll(cube.buildDrillDownChain(usedDimensions, dimension));
			}
		}

		void processAttributes() {
			for (String attribute : attributes) {
				AttributeResolver resolver = reportingConfiguration.getAttributeResolver(attribute);
				if (resolver == null)
					throw new QueryException("Cube does not contain resolver for '" + attribute + "'");

				List<String> keyComponents = reportingConfiguration.getKeyForResolver(resolver);

				boolean usingStoredDimension = false;
				for (String keyComponent : keyComponents) {
					if (predicates != null && predicates.get(keyComponent) instanceof AggregationQuery.QueryPredicateEq) {
						if (usingStoredDimension)
							throw new QueryException("Incorrect filter: using 'equals' predicate when prefix of this " +
									"compound key is not fully defined");
						else
							keyConstants.put(keyComponent,
									((AggregationQuery.QueryPredicateEq) predicates.get(keyComponent)).value);
					} else {
						storedDimensions.add(keyComponent);
						usingStoredDimension = true;
					}
				}

				resolverKeys.put(resolver, keyComponents);
				Class<?> attributeType = reportingConfiguration.getAttributeType(attribute);
				attributeTypes.put(attribute, attributeType);

				if (all(keyComponents, in(predicates.keySet()))) {
					filterAttributes.add(attribute);
					filterAttributeTypes.put(attribute, attributeType);
				}
			}
		}

		void processMeasures() {
			Set<String> measures = newHashSet();
			List<String> queryComputedMeasures = newArrayList();

			for (String queryMeasure : queryMeasures) {
				if (structure.containsOutputField(queryMeasure)) {
					measures.add(queryMeasure);
				} else if (reportingConfiguration.containsComputedMeasure(queryMeasure)) {
					ReportingDSLExpression expression = reportingConfiguration.getExpressionForMeasure(queryMeasure);
					measures.addAll(expression.getMeasureDependencies());
					queryComputedMeasures.add(queryMeasure);
				} else {
					throw new QueryException("Cube does not contain measure with name '" + queryMeasure + "'");
				}
			}

			drillDowns = cube.getAvailableDrillDowns(storedDimensions, queryPredicates, measures);
			storedMeasures = drillDowns.getMeasures();

			for (String computedMeasure : queryComputedMeasures) {
				if (all(reportingConfiguration.getComputedMeasureDependencies(computedMeasure), in(storedMeasures)))
					computedMeasures.add(computedMeasure);
			}
		}

		void processOrdering() {
			if (ordering == null)
				return;

			additionalSortingRequired = computedMeasures.contains(ordering.getPropertyName())
					|| attributeTypes.containsKey(ordering.getPropertyName());

			if (!storedDimensions.contains(ordering.getPropertyName())
					&& !storedMeasures.contains(ordering.getPropertyName()) && !additionalSortingRequired) {
				throw new QueryException("Ordering is specified by not requested field");
			}

			if (!additionalSortingRequired)
				query.ordering(ordering);
		}

		Class<QueryResultPlaceholder> createResultClass() {
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
				computeSequence.add(set(getter(self(), computedMeasure),
						reportingConfiguration.getComputedMeasureExpression(computedMeasure)));
			}
			builder.method("computeMeasures", computeSequence);
			return builder.defineClass();
		}

		StreamConsumers.ToList<QueryResultPlaceholder> queryCube() {
			StreamConsumers.ToList<QueryResultPlaceholder> consumerStream = StreamConsumers.toList(eventloop);
			StreamProducer<QueryResultPlaceholder> queryResultProducer = cube.query(resultClass, query);
			queryResultProducer.streamTo(consumerStream);
			return consumerStream;
		}

		@SuppressWarnings("unchecked")
		Comparator<QueryResultPlaceholder> generateComparator(String fieldName, boolean ascending,
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

		@SuppressWarnings("unchecked")
		void processResults(List<QueryResultPlaceholder> results, ResultCallback<QueryResult> callback) {
			Class<?> filterAttributesClass = createFilterAttributesClass();
			Object filterAttributesPlaceholder = instantiate(filterAttributesClass);

			computeMeasures(results);
			resolver.resolve((List) results, resultClass, attributeTypes, resolverKeys, keyConstants);
			resolver.resolve(singletonList(filterAttributesPlaceholder), filterAttributesClass, filterAttributeTypes,
					resolverKeys, keyConstants);
			results = performSearch(results);
			sort(results);
			TotalsPlaceholder totalsPlaceholder = computeTotals(results);

			List<String> resultMeasures;
			if (ignoreMeasures)
				resultMeasures = newArrayList();
			else
				resultMeasures = newArrayList(filter(concat(storedMeasures, computedMeasures),
						new Predicate<String>() {
							@Override
							public boolean apply(String measure) {
								return queryMeasures.contains(measure);
							}
						}));

			int count = results.size();
			callback.onResult(new QueryResult(applyLimitAndOffset(results), resultClass, totalsPlaceholder,
					count, drillDowns.getDrillDowns(), newArrayList(storedDimensions), attributes, resultMeasures,
					filterAttributesPlaceholder, filterAttributes, filterAttributesClass));
		}

		List performSearch(List results) {
			if (searchString == null)
				return results;

			Predicate searchPredicate = createSearchPredicate(searchString,
					newArrayList(concat(storedDimensions, attributes)), resultClass);
			return newArrayList(filter(results, searchPredicate));
		}

		List applyLimitAndOffset(List results) {
			int start = offset == null ? 0 : offset;
			int end;

			if (limit == null)
				end = results.size();
			else if (start + limit > results.size())
				end = results.size();
			else
				end = start + limit;

			return results.subList(start, end);
		}

		TotalsPlaceholder computeTotals(List<QueryResultPlaceholder> results) {
			TotalsPlaceholder totalsPlaceholder = createTotalsPlaceholder(resultClass, storedMeasures, computedMeasures);
			for (QueryResultPlaceholder record : results) {
				totalsPlaceholder.accumulate(record);
			}
			totalsPlaceholder.computeMeasures();
			return totalsPlaceholder;
		}

		TotalsPlaceholder createTotalsPlaceholder(Class<?> inputClass, Set<String> requestedStoredFields,
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
				computeSequence.add(set(getter(self(), computedMeasure),
						reportingConfiguration.getComputedMeasureExpression(computedMeasure)));
			}
			builder.method("computeMeasures", computeSequence);

			return builder.newInstance();
		}

		void computeMeasures(List<QueryResultPlaceholder> results) {
			for (QueryResultPlaceholder queryResult : results) {
				queryResult.computeMeasures();
			}
		}

		void sort(List<QueryResultPlaceholder> results) {
			if (comparator != null) {
				Collections.sort(results, comparator);
			}
		}

		Class createFilterAttributesClass() {
			AsmBuilder<Object> builder = new AsmBuilder<>(classLoader, Object.class);
			for (String filterAttribute : filterAttributes) {
				builder.field(filterAttribute, attributeTypes.get(filterAttribute));
			}
			return builder.defineClass();
		}

		Predicate createSearchPredicate(String searchString, List<String> properties, Class recordClass) {
			AsmBuilder<Predicate> builder = new AsmBuilder<>(classLoader, Predicate.class);

			PredicateDefOr predicate = or();

			for (String property : properties) {
				Expression propertyValue = cast(getter(cast(arg(0), recordClass), property), Object.class);

				predicate.add(cmpEq(
						choice(isNull(propertyValue),
								value(false),
								call(call(propertyValue, "toString"), "contains", cast(value(searchString), CharSequence.class))),
						value(true)));
			}

			builder.method("apply", boolean.class, singletonList(Object.class), predicate);
			return builder.newInstance();
		}
	}
}

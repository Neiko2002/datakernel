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

package io.datakernel.cube;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.MoreObjects;
import com.google.common.base.Predicate;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import io.datakernel.aggregation_db.*;
import io.datakernel.aggregation_db.api.QueryException;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ForwardingCompletionCallback;
import io.datakernel.async.ForwardingResultCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.codegen.AsmBuilder;
import io.datakernel.codegen.ExpressionComparator;
import io.datakernel.codegen.utils.DefiningClassLoader;
import io.datakernel.cube.api.AttributeResolver;
import io.datakernel.cube.api.ReportingConfiguration;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.jmx.ConcurrentJmxMBean;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.processor.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Iterables.*;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static io.datakernel.codegen.Expressions.*;
import static java.util.Collections.sort;

/**
 * Represents an OLAP cube. Provides methods for loading and querying data.
 * Also provides functionality for managing aggregations.
 */
@SuppressWarnings("unchecked")
public final class Cube implements ConcurrentJmxMBean {
	private static final Logger logger = LoggerFactory.getLogger(Cube.class);

	private final Eventloop eventloop;
	private final ExecutorService executorService;
	private final DefiningClassLoader classLoader;
	private final CubeMetadataStorage cubeMetadataStorage;
	private final AggregationChunkStorage aggregationChunkStorage;
	private int aggregationChunkSize;
	private int sorterItemsInMemory;
	private int sorterBlockSize;
	private boolean ignoreChunkReadingExceptions;

	private final AggregationStructure structure;
	private ReportingConfiguration reportingConfiguration = new ReportingConfiguration();

	private Map<String, Aggregation> aggregations = new LinkedHashMap<>();
	private AggregationKeyRelationships childParentRelationships;

	private int lastRevisionId;

	/**
	 * Instantiates a cube with the specified structure, that runs in a given event loop,
	 * uses the specified class loader for creating dynamic classes, saves data and metadata to given storages,
	 * and uses the specified parameters.
	 *
	 * @param eventloop               event loop, in which the cube is to run
	 * @param classLoader             class loader for defining dynamic classes
	 * @param cubeMetadataStorage     storage for aggregations metadata
	 * @param aggregationChunkStorage storage for data chunks
	 * @param structure               structure of a cube
	 * @param sorterItemsInMemory     maximum number of records that can stay in memory while sorting
	 * @param aggregationChunkSize    maximum size of aggregation chunk
	 */
	public Cube(Eventloop eventloop, ExecutorService executorService, DefiningClassLoader classLoader,
	            CubeMetadataStorage cubeMetadataStorage, AggregationChunkStorage aggregationChunkStorage,
	            AggregationStructure structure, int sorterItemsInMemory, int sorterBlockSize, int aggregationChunkSize) {
		this.eventloop = eventloop;
		this.executorService = executorService;
		this.classLoader = classLoader;
		this.cubeMetadataStorage = cubeMetadataStorage;
		this.aggregationChunkStorage = aggregationChunkStorage;
		this.structure = structure;
		this.aggregationChunkSize = aggregationChunkSize;
		this.sorterItemsInMemory = sorterItemsInMemory;
		this.sorterBlockSize = sorterBlockSize;
	}

	public void setChildParentRelationships(Map<String, String> childParentRelationships) {
		this.childParentRelationships = new AggregationKeyRelationships(childParentRelationships);
	}

	public Map<String, Aggregation> getAggregations() {
		return aggregations;
	}

	public AggregationStructure getStructure() {
		return structure;
	}

	public ReportingConfiguration getReportingConfiguration() {
		return reportingConfiguration;
	}

	public void setReportingConfiguration(ReportingConfiguration reportingConfiguration) {
		this.reportingConfiguration = reportingConfiguration;
		initResolverKeys();
	}

	public void initResolverKeys() {
		for (Map.Entry<String, String> attributeDimension : reportingConfiguration.getAttributeDimensions().entrySet()) {
			List<String> key = buildDrillDownChain(attributeDimension.getValue());
			reportingConfiguration.setKeyForAttribute(attributeDimension.getKey(), key);
		}
	}

	public Map<String, AttributeResolver> getResolvers() {
		return reportingConfiguration == null ? new HashMap<String, AttributeResolver>() : reportingConfiguration.getResolvers();
	}

	/**
	 * Creates an {@link Aggregation} with the specified metadata and adds it to the collection of aggregations managed by this cube.
	 *
	 * @param aggregationMetadata metadata of aggregation
	 */
	public void addAggregation(String aggregationId, AggregationMetadata aggregationMetadata, String partitioningKey) {
		AggregationMetadataStorage aggregationMetadataStorage = cubeMetadataStorage.aggregationMetadataStorage(aggregationId, aggregationMetadata, structure);
		Aggregation aggregation = new Aggregation(eventloop, executorService, classLoader, aggregationMetadataStorage,
				aggregationChunkStorage, aggregationMetadata, structure, partitioningKey, sorterItemsInMemory,
				sorterBlockSize, aggregationChunkSize);
		checkArgument(!aggregations.containsKey(aggregationId), "Aggregation '%s' is already defined", aggregationId);
		aggregations.put(aggregationId, aggregation);
		logger.info("Added aggregation {} for id '{}'", aggregation, aggregationId);
	}

	public void addAggregation(String aggregationId, AggregationMetadata aggregationMetadata) {
		addAggregation(aggregationId, aggregationMetadata, null);
	}

	public void incrementLastRevisionId() {
		++lastRevisionId;
	}

	public int getLastRevisionId() {
		return lastRevisionId;
	}

	public void setLastRevisionId(int lastRevisionId) {
		this.lastRevisionId = lastRevisionId;
	}

	public <T> StreamConsumer<T> consumer(Class<T> inputClass, List<String> dimensions, List<String> measures,
	                                      ResultCallback<Multimap<AggregationMetadata, AggregationChunk.NewChunk>> callback) {
		return consumer(inputClass, dimensions, measures, null, null, callback);
	}

	private Collection<Aggregation> findAggregationsForWriting(final AggregationQuery.Predicates predicates) {
		Collection<Aggregation> allAggregations = aggregations.values();

		if (predicates == null)
			// return aggregations without predicates
			return newArrayList(filter(allAggregations, new Predicate<Aggregation>() {
				@Override
				public boolean apply(Aggregation aggregation) {
					return !aggregation.hasPredicates();
				}
			}));
		else
			// return aggregations with matching predicates
			return newArrayList(filter(allAggregations, new Predicate<Aggregation>() {
				@Override
				public boolean apply(Aggregation aggregation) {
					return aggregation.matchQueryPredicates(predicates);
				}
			}));
	}

	/**
	 * Provides a {@link StreamConsumer} for streaming data to this cube.
	 * The returned {@link StreamConsumer} writes to {@link Aggregation}'s chosen using the specified dimensions, measures and input class.
	 *
	 * @param inputClass class of input records
	 * @param dimensions list of dimension names
	 * @param measures   list of measure names
	 * @param callback   callback which is called when records are committed
	 * @param <T>        data records type
	 * @return consumer for streaming data to cube
	 */
	public <T> StreamConsumer<T> consumer(Class<T> inputClass, List<String> dimensions, List<String> measures,
	                                      Map<String, String> outputToInputFields, AggregationQuery.Predicates predicates,
	                                      final ResultCallback<Multimap<AggregationMetadata, AggregationChunk.NewChunk>> callback) {
		logger.info("Started consuming data. Dimensions: {}. Measures: {}", dimensions, measures);

		final StreamSplitter<T> streamSplitter = new StreamSplitter<>(eventloop);
		final Multimap<AggregationMetadata, AggregationChunk.NewChunk> resultChunks = LinkedHashMultimap.create();
		final int[] aggregationsDone = {0};
		final int[] streamSplitterOutputs = {0};

		Collection<Aggregation> preparedAggregations = findAggregationsForWriting(predicates);

		for (final Aggregation aggregation : preparedAggregations) {
			if (!aggregation.allKeysIn(dimensions))
				continue;

			List<String> aggregationMeasures = aggregation.getAggregationFieldsForConsumer(measures);
			if (aggregationMeasures.isEmpty())
				continue;

			StreamConsumer<T> groupReducer = aggregation.consumer(inputClass, aggregationMeasures, outputToInputFields,
					new ForwardingResultCallback<List<AggregationChunk.NewChunk>>(callback) {
						@Override
						public void onResult(List<AggregationChunk.NewChunk> chunks) {
							resultChunks.putAll(aggregation.getAggregationMetadata(), chunks);
							++aggregationsDone[0];
							if (aggregationsDone[0] == streamSplitterOutputs[0]) {
								callback.onResult(resultChunks);
							}
						}
					});

			streamSplitter.newOutput().streamTo(groupReducer);
			++streamSplitterOutputs[0];
		}

		return streamSplitter.getInput();
	}

	private Comparator<Aggregation> aggregationCostComparator(final AggregationQuery query) {
		return new Comparator<Aggregation>() {
			@Override
			public int compare(Aggregation aggregation1, Aggregation aggregation2) {
				return Double.compare(aggregation1.getCost(query), aggregation2.getCost(query));
			}
		};
	}

	private Comparator<Aggregation> descendingNumberOfPredicatesComparator() {
		return new Comparator<Aggregation>() {
			@Override
			public int compare(Aggregation aggregation1, Aggregation aggregation2) {
				return Integer.compare(aggregation2.getNumberOfPredicates(), aggregation1.getNumberOfPredicates());
			}
		};
	}

	private Map<Aggregation, List<String>> findAggregationsForQuery(final AggregationQuery query) {
		Map<Aggregation, List<String>> aggregationIdToAppliedPredicateKeys = new HashMap<>();
		Map<Aggregation, List<String>> aggregationToAppliedPredicateKeys = new LinkedHashMap<>();
		List<Aggregation> allAggregations = newArrayList(aggregations.values());
		Collections.sort(allAggregations, descendingNumberOfPredicatesComparator());

		List<Aggregation> aggregationsThatSatisfyPredicates = newArrayList();
		List<Aggregation> aggregationsWithoutPredicates = newArrayList();

		for (Aggregation aggregation : allAggregations) {
			AggregationFilteringResult result = aggregation.applyQueryPredicates(query, structure);
			aggregationIdToAppliedPredicateKeys.put(aggregation, result.getAppliedPredicateKeys());
			boolean satisfiesPredicates = result.isMatches();

			if (satisfiesPredicates) {
				aggregationsThatSatisfyPredicates.add(aggregation);
			} else if (!aggregation.hasPredicates()) {
				aggregationsWithoutPredicates.add(aggregation);
			}
		}

		Comparator<Aggregation> aggregationCostComparator = aggregationCostComparator(query);

		sort(aggregationsWithoutPredicates, aggregationCostComparator);

		List<Aggregation> resultAggregations = newArrayList(concat(aggregationsThatSatisfyPredicates, aggregationsWithoutPredicates));

		for (Aggregation aggregation : resultAggregations) {
			aggregationToAppliedPredicateKeys.put(aggregation, aggregationIdToAppliedPredicateKeys.get(aggregation));
		}

		return aggregationToAppliedPredicateKeys;
	}

	public CubeQuery getQueryWithoutAppliedPredicateKeys(CubeQuery query, List<String> appliedPredicateKeys) {
		Map<String, AggregationQuery.Predicate> filteredQueryPredicates = new LinkedHashMap<>();
		for (Map.Entry<String, AggregationQuery.Predicate> queryPredicateEntry : query.getPredicates().asMap().entrySet()) {
			if (!appliedPredicateKeys.contains(queryPredicateEntry.getKey()))
				filteredQueryPredicates.put(queryPredicateEntry.getKey(), queryPredicateEntry.getValue());
		}

		return new CubeQuery(query.getResultDimensions(), query.getResultMeasures(),
				AggregationQuery.Predicates.fromMap(filteredQueryPredicates), query.getOrderings());
	}

	/**
	 * Returns a {@link StreamProducer} of the records retrieved from cube for the specified query.
	 *
	 * @param <T>         type of output objects
	 * @param resultClass class of output records
	 * @param query       query
	 * @return producer that streams query results
	 */
	public <T> StreamProducer<T> query(Class<T> resultClass, CubeQuery query) {
		StreamReducer<Comparable, T, Object> streamReducer = new StreamReducer<>(eventloop, Ordering.natural());

		Map<Aggregation, List<String>> aggregationsToAppliedPredicateKeys = findAggregationsForQuery(query.getAggregationQuery());

		List<String> queryMeasures = newArrayList(query.getResultMeasures());
		List<String> resultDimensions = query.getResultDimensions();
		Class resultKeyClass = structure.createKeyClass(resultDimensions);

		CubeQueryPlan queryPlan = new CubeQueryPlan();

		for (Map.Entry<Aggregation, List<String>> entry : aggregationsToAppliedPredicateKeys.entrySet()) {
			Aggregation aggregation = entry.getKey();
			CubeQuery filteredQuery = getQueryWithoutAppliedPredicateKeys(query, entry.getValue());
			if (queryMeasures.isEmpty())
				break;
			if (!aggregation.containsKeys(filteredQuery.getAllKeys()))
				continue;
			List<String> aggregationMeasures = aggregation.getAggregationFieldsForQuery(queryMeasures);
			queryPlan.addAggregationMeasures(aggregation, aggregationMeasures);

			if (aggregationMeasures.isEmpty())
				continue;

			Class aggregationClass = structure.createRecordClass(aggregation.getKeys(), aggregationMeasures);

			StreamProducer<T> queryResultProducer = aggregation.query(filteredQuery.getAggregationQuery(),
					aggregationMeasures, aggregationClass);

			Function keyFunction = structure.createKeyFunction(aggregationClass, resultKeyClass, resultDimensions);

			StreamReducers.Reducer reducer = aggregation.aggregationReducer(aggregationClass, resultClass,
					resultDimensions, aggregationMeasures);

			StreamConsumer streamReducerInput = streamReducer.newInput(keyFunction, reducer);

			queryResultProducer.streamTo(streamReducerInput);

			queryMeasures = newArrayList(filter(queryMeasures, not(in(aggregation.getFields()))));
		}

		if (!queryMeasures.isEmpty()) {
			logger.info("Could not build query plan for {}. Incomplete plan: {}", query, queryPlan);
			throw new QueryException("Could not find suitable aggregation(s)");
		} else
			logger.info("Built query plan for {}: {}", query, queryPlan);

		return getOrderedResultStream(query, resultClass, streamReducer,
				query.getResultDimensions(), query.getResultMeasures());
	}

	public void loadChunks(CompletionCallback callback) {
		logger.info("Loading chunks");
		loadChunks(new ArrayList<>(this.aggregations.values()).iterator(), callback);
	}

	private void loadChunks(final Iterator<Aggregation> iterator, final CompletionCallback callback) {
		if (iterator.hasNext()) {
			iterator.next().loadChunks(new ForwardingCompletionCallback(callback) {
				@Override
				public void onComplete() {
					loadChunks(iterator, callback);
				}
			});
		} else {
			logger.info("Loading chunks completed");
			callback.onComplete();
		}
	}

	public void consolidate(int maxChunksToConsolidate, ResultCallback<Boolean> callback) {
		logger.info("Launching consolidation");
		consolidate(maxChunksToConsolidate, false, new ArrayList<>(this.aggregations.values()).iterator(), callback);
	}

	private void consolidate(final int maxChunksToConsolidate, final boolean found,
	                         final Iterator<Aggregation> iterator, final ResultCallback<Boolean> callback) {
		eventloop.post(new Runnable() {
			@Override
			public void run() {
				if (iterator.hasNext()) {
					final Aggregation aggregation = iterator.next();
					aggregation.consolidate(maxChunksToConsolidate, new ResultCallback<Boolean>() {
						@Override
						public void onResult(Boolean result) {
							consolidate(maxChunksToConsolidate, result || found, iterator, callback);
						}

						@Override
						public void onException(Exception exception) {
							logger.error("Consolidating aggregation '{}' failed", aggregation, exception);
							consolidate(maxChunksToConsolidate, found, iterator, callback);
						}
					});
				} else {
					callback.onResult(found);
				}
			}
		});
	}

	public static class DrillDownsAndChains {
		public Set<DrillDown> drillDowns;
		public Set<List<String>> chains;

		public DrillDownsAndChains(Set<DrillDown> drillDowns, Set<List<String>> chains) {
			this.drillDowns = drillDowns;
			this.chains = chains;
		}
	}

	public DrillDownsAndChains getDrillDownsAndChains(Set<String> dimensions, AggregationQuery.Predicates predicates,
	                                                  Set<String> measures) {
		Set<DrillDown> drillDowns = newHashSet();
		Set<List<String>> chains = newHashSet();

		AggregationQuery query = new AggregationQuery(newArrayList(dimensions), newArrayList(measures), predicates);

		List<String> queryDimensions = getQueryDimensions(dimensions, predicates.asCollection());

		for (Aggregation aggregation : aggregations.values()) {
			Set<String> aggregationMeasures = newHashSet();
			aggregationMeasures.addAll(aggregation.getFields());

			if (!all(queryDimensions, in(aggregation.getKeys())) || !any(measures, in(aggregationMeasures)) ||
					aggregation.hasPredicates() && !aggregation.applyQueryPredicates(query, structure).isMatches())
				continue;

			Set<String> availableMeasures = newHashSet();
			Sets.intersection(aggregationMeasures, measures).copyInto(availableMeasures);

			Iterable<String> filteredDimensions = filter(aggregation.getKeys(), not(in(queryDimensions)));
			Set<List<String>> filteredChains = childParentRelationships.buildDrillDownChains(newHashSet(queryDimensions), filteredDimensions);
			Set<List<String>> allChains = childParentRelationships.buildDrillDownChains(Sets.<String>newHashSet(), aggregation.getKeys());

			for (List<String> drillDownChain : filteredChains) {
				drillDowns.add(new DrillDown(drillDownChain, availableMeasures));
			}

			chains.addAll(allChains);
		}

		Set<List<String>> longestChains = childParentRelationships.buildLongestChains(chains);

		return new DrillDownsAndChains(drillDowns, longestChains);
	}

	private List<String> getQueryDimensions(Iterable<String> dimensions,
	                                        Iterable<AggregationQuery.Predicate> predicates) {
		Set<String> eqPredicateDimensions = newHashSet();

		for (AggregationQuery.Predicate predicate : predicates) {
			if (predicate instanceof AggregationQuery.PredicateEq) {
				eqPredicateDimensions.add(predicate.key);
			}
		}

		return newArrayList(concat(dimensions, eqPredicateDimensions));
	}

	public List<String> buildDrillDownChain(Set<String> usedDimensions, String dimension) {
		return childParentRelationships.buildDrillDownChain(usedDimensions, dimension);
	}

	public List<String> buildDrillDownChain(String dimension) {
		return buildDrillDownChain(Sets.<String>newHashSet(), dimension);
	}

	public Set<String> getAvailableMeasures(Set<String> dimensions, AggregationQuery.Predicates predicates,
	                                        Set<String> measures) {
		Set<String> availableMeasures = newHashSet();

		AggregationQuery query = new AggregationQuery(newArrayList(dimensions), newArrayList(measures), predicates);

		List<String> queryDimensions = getQueryDimensions(dimensions, predicates.asCollection());

		for (Aggregation aggregation : aggregations.values()) {
			Set<String> aggregationMeasures = newHashSet();
			aggregationMeasures.addAll(aggregation.getFields());

			if (!all(queryDimensions, in(aggregation.getKeys())) || !any(measures, in(aggregationMeasures)) ||
					aggregation.hasPredicates() && !aggregation.applyQueryPredicates(query, structure).isMatches())
				continue;

			Sets.intersection(aggregationMeasures, measures).copyInto(availableMeasures);
		}

		return availableMeasures;
	}

	private <T> StreamProducer<T> getOrderedResultStream(CubeQuery query, Class<T> resultClass,
	                                                     StreamReducer<Comparable, T, Object> rawResultStream,
	                                                     List<String> dimensions, List<String> measures) {
		if (queryRequiresSorting(query)) {
			Comparator fieldComparator = createFieldComparator(query, resultClass);
			Path path = Paths.get("sorterStorage", "%d.part");
			BufferSerializer bufferSerializer = structure.createBufferSerializer(resultClass, dimensions, measures);
			StreamMergeSorterStorage sorterStorage = new StreamMergeSorterStorageImpl(eventloop, executorService,
					bufferSerializer, path, sorterBlockSize);
			StreamSorter sorter = new StreamSorter(eventloop, sorterStorage, Functions.identity(),
					fieldComparator, false, sorterItemsInMemory);
			rawResultStream.getOutput().streamTo(sorter.getInput());
			return sorter.getOutput();
		} else {
			return rawResultStream.getOutput();
		}
	}

	public Comparator createFieldComparator(CubeQuery query, Class<?> fieldClass) {
		logger.trace("Creating field comparator for query {}", query.toString());
		AsmBuilder<Comparator> builder = new AsmBuilder<>(classLoader, Comparator.class);
		ExpressionComparator comparator = comparator();
		List<CubeQuery.Ordering> orderings = query.getOrderings();

		for (CubeQuery.Ordering ordering : orderings) {
			boolean isAsc = ordering.isAsc();
			String field = ordering.getPropertyName();
			if (isAsc)
				comparator.add(
						getter(cast(arg(0), fieldClass), field),
						getter(cast(arg(1), fieldClass), field));
			else
				comparator.add(
						getter(cast(arg(1), fieldClass), field),
						getter(cast(arg(0), fieldClass), field));
		}

		builder.method("compare", comparator);

		return builder.newInstance();
	}

	private boolean queryRequiresSorting(CubeQuery query) {
		return query.getOrderings().size() != 0;
	}

	@Override
	public String toString() {
		return MoreObjects.toStringHelper(this)
				.add("structure", structure)
				.add("aggregations", aggregations)
				.add("lastRevisionId", lastRevisionId)
				.toString();
	}

	// jmx

	@JmxAttribute
	public void setIgnoreChunkReadingExceptions(boolean ignoreChunkReadingExceptions) {
		this.ignoreChunkReadingExceptions = ignoreChunkReadingExceptions;
		for (Aggregation aggregation : aggregations.values()) {
			aggregation.setIgnoreChunkReadingExceptions(ignoreChunkReadingExceptions);
		}
	}

	@JmxAttribute
	public boolean getIgnoreChunkReadingExceptions() {
		return ignoreChunkReadingExceptions;
	}

	@JmxAttribute
	public void setChunkSize(int chunkSize) {
		this.aggregationChunkSize = chunkSize;
		for (Aggregation aggregation : aggregations.values()) {
			aggregation.setAggregationChunkSize(chunkSize);
		}
	}

	@JmxAttribute
	public int getChunkSize() {
		return aggregationChunkSize;
	}

	@JmxAttribute
	public void setSorterItemsInMemory(int sorterItemsInMemory) {
		this.sorterItemsInMemory = sorterItemsInMemory;
		for (Aggregation aggregation : aggregations.values()) {
			aggregation.setSorterItemsInMemory(sorterItemsInMemory);
		}
	}

	@JmxAttribute
	public int getSorterItemsInMemory() {
		return sorterItemsInMemory;
	}

	@JmxAttribute
	public void setSorterBlockSize(int sorterBlockSize) {
		this.sorterBlockSize = sorterBlockSize;
		for (Aggregation aggregation : aggregations.values()) {
			aggregation.setSorterBlockSize(sorterBlockSize);
		}
	}

	@JmxAttribute
	public int getSorterBlockSize() {
		return sorterBlockSize;
	}

	@Override
	public Executor getJmxExecutor() {
		return eventloop;
	}
}
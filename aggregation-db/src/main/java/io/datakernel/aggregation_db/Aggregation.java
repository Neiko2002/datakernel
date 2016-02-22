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

package io.datakernel.aggregation_db;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.collect.Ordering;
import io.datakernel.aggregation_db.AggregationMetadataStorage.LoadedChunks;
import io.datakernel.aggregation_db.processor.ProcessorFactory;
import io.datakernel.async.*;
import io.datakernel.codegen.AsmBuilder;
import io.datakernel.codegen.PredicateDefAnd;
import io.datakernel.codegen.utils.DefiningClassLoader;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.stream.ErrorIgnoringTransformer;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;
import io.datakernel.stream.processor.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutorService;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Predicates.in;
import static com.google.common.collect.Iterables.*;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.intersection;
import static com.google.common.collect.Sets.newHashSet;
import static com.google.common.collect.Sets.newLinkedHashSet;
import static io.datakernel.codegen.Expressions.*;
import static java.util.Arrays.asList;

/**
 * Represents an aggregation, which aggregates data using custom reducer and preaggregator.
 * Provides methods for loading and querying data.
 */
@SuppressWarnings("unchecked")
public class Aggregation {
	private static final Logger logger = LoggerFactory.getLogger(Aggregation.class);
	private static final Joiner JOINER = Joiner.on(", ");

	public static final int DEFAULT_SORTER_ITEMS_IN_MEMORY = 1_000_000;
	public static final int DEFAULT_AGGREGATION_CHUNK_SIZE = 1_000_000;
	public static final int DEFAULT_SORTER_BLOCK_SIZE = 256 * 1024;

	private final Eventloop eventloop;
	private final ExecutorService executorService;
	private final DefiningClassLoader classLoader;
	private final AggregationMetadataStorage metadataStorage;
	private final AggregationChunkStorage aggregationChunkStorage;
	private final AggregationMetadata aggregationMetadata;
	private final String partitioningKey;
	private int aggregationChunkSize;
	private int sorterItemsInMemory;
	private int sorterBlockSize;
	private boolean ignoreChunkReadingExceptions;

	private final AggregationStructure structure;

	private final ProcessorFactory processorFactory;

	private final Map<Long, AggregationChunk> chunks = new LinkedHashMap<>();

	private int lastRevisionId;

	private ListenableCompletionCallback loadChunksCallback;

	/**
	 * Instantiates an aggregation with the specified structure, that runs in a given event loop,
	 * uses the specified class loader for creating dynamic classes, saves data and metadata to given storages,
	 * and uses the specified parameters.
	 *
	 * @param eventloop               event loop, in which the aggregation is to run
	 * @param classLoader             class loader for defining dynamic classes
	 * @param metadataStorage         storage for aggregations metadata
	 * @param aggregationChunkStorage storage for data chunks
	 * @param aggregationMetadata     metadata of the aggregation
	 * @param structure               structure of an aggregation
	 * @param aggregationChunkSize    maximum size of aggregation chunk
	 * @param sorterItemsInMemory     maximum number of records that can stay in memory while sorting
	 */
	public Aggregation(Eventloop eventloop, ExecutorService executorService, DefiningClassLoader classLoader,
	                   AggregationMetadataStorage metadataStorage, AggregationChunkStorage aggregationChunkStorage,
	                   AggregationMetadata aggregationMetadata, AggregationStructure structure,
	                   String partitioningKey, int sorterItemsInMemory, int sorterBlockSize, int aggregationChunkSize) {
		checkArgument(partitioningKey == null || aggregationMetadata.getKeys().contains(partitioningKey));
		this.eventloop = eventloop;
		this.executorService = executorService;
		this.classLoader = classLoader;
		this.metadataStorage = metadataStorage;
		this.aggregationChunkStorage = aggregationChunkStorage;
		this.aggregationMetadata = aggregationMetadata;
		this.partitioningKey = partitioningKey;
		this.sorterItemsInMemory = sorterItemsInMemory;
		this.sorterBlockSize = sorterBlockSize;
		this.aggregationChunkSize = aggregationChunkSize;
		this.structure = structure;
		this.processorFactory = new ProcessorFactory(classLoader, structure);
	}

	/**
	 * Instantiates an aggregation with the specified structure, that runs in a given event loop,
	 * uses the specified class loader for creating dynamic classes, saves data and metadata to given storages.
	 * Maximum size of chunk is 1,000,000 bytes.
	 * No more than 1,000,000 records stay in memory while sorting.
	 * Maximum duration of consolidation attempt is 30 minutes.
	 * Consolidated chunks become available for removal in 10 minutes from consolidation.
	 *
	 * @param eventloop               event loop, in which the aggregation is to run
	 * @param classLoader             class loader for defining dynamic classes
	 * @param metadataStorage         storage for persisting aggregation metadata
	 * @param aggregationChunkStorage storage for data chunks
	 * @param aggregationMetadata     metadata of the aggregation
	 * @param structure               structure of an aggregation
	 */
	public Aggregation(Eventloop eventloop, ExecutorService executorService, DefiningClassLoader classLoader,
	                   AggregationMetadataStorage metadataStorage, AggregationChunkStorage aggregationChunkStorage,
	                   AggregationMetadata aggregationMetadata, AggregationStructure structure) {
		this(eventloop, executorService, classLoader, metadataStorage, aggregationChunkStorage, aggregationMetadata,
				structure, null, DEFAULT_SORTER_ITEMS_IN_MEMORY, DEFAULT_SORTER_BLOCK_SIZE,
				DEFAULT_AGGREGATION_CHUNK_SIZE);
	}

	public List<String> getAggregationFieldsForConsumer(List<String> fields) {
		return newArrayList(filter(getFields(), in(fields)));
	}

	public List<String> getAggregationFieldsForQuery(List<String> queryFields) {
		return newArrayList(filter(queryFields, in(getFields())));
	}

	public boolean allKeysIn(List<String> requestedKeys) {
		return aggregationMetadata.allKeysIn(requestedKeys);
	}

	public boolean containsKeys(List<String> requestedKeys) {
		return aggregationMetadata.containsKeys(requestedKeys);
	}

	public Map<Long, AggregationChunk> getChunks() {
		return Collections.unmodifiableMap(chunks);
	}

	public AggregationMetadata getAggregationMetadata() {
		return aggregationMetadata;
	}

	public void addToIndex(AggregationChunk chunk) {
		aggregationMetadata.addToIndex(chunk);
		chunks.put(chunk.getChunkId(), chunk);
	}

	public boolean hasPredicates() {
		return aggregationMetadata.hasPredicates();
	}

	public boolean matchQueryPredicates(AggregationQuery.Predicates predicates) {
		return aggregationMetadata.matchQueryPredicates(predicates);
	}

	public AggregationFilteringResult applyQueryPredicates(AggregationQuery query, AggregationStructure structure) {
		return aggregationMetadata.applyQueryPredicates(query, structure);
	}

	public void removeFromIndex(AggregationChunk chunk) {
		aggregationMetadata.removeFromIndex(chunk);
		chunks.remove(chunk.getChunkId());
	}

	public List<String> getKeys() {
		return aggregationMetadata.getKeys();
	}

	public int getNumberOfPredicates() {
		return aggregationMetadata.getNumberOfPredicates();
	}

	public List<String> getFields() {
		return aggregationMetadata.getFields();
	}

	public double getCost(AggregationQuery query) {
		return aggregationMetadata.getCost(query);
	}

	public AggregationQuery.Predicates getAggregationPredicates() {
		return aggregationMetadata.getAggregationPredicates();
	}

	public AggregationStructure getStructure() {
		return structure;
	}

	public int getLastRevisionId() {
		return lastRevisionId;
	}

	public void setLastRevisionId(int lastRevisionId) {
		this.lastRevisionId = lastRevisionId;
	}

	public void incrementLastRevisionId() {
		++lastRevisionId;
	}

	public StreamReducers.Reducer aggregationReducer(Class<?> inputClass, Class<?> outputClass, List<String> keys,
	                                                 List<String> fields) {
		return processorFactory.aggregationReducer(inputClass, outputClass, keys, fields);
	}

	public static <T> StreamConsumer<T> createChunker(PartitioningStrategy partitioningStrategy, Eventloop eventloop,
	                                                  List<String> keys, List<String> fields,
	                                                  Class<T> recordClass, AggregationChunkStorage storage,
	                                                  AggregationMetadataStorage metadataStorage, int chunkSize,
	                                                  ResultCallback<List<AggregationChunk.NewChunk>> chunksCallback) {
		if (partitioningStrategy == null)
			return new AggregationChunker<>(eventloop, keys, fields, recordClass, storage,
					metadataStorage, chunkSize, chunksCallback);

		return new PartitioningAggregationChunker<>(partitioningStrategy, eventloop, keys, fields,
				recordClass, storage, metadataStorage, chunkSize, chunksCallback).getInput();
	}

	private PartitioningStrategy createPartitioningStrategy(Class recordClass) {
		if (partitioningKey == null)
			return null;

		AsmBuilder<PartitioningStrategy> builder = new AsmBuilder<>(classLoader, PartitioningStrategy.class);
		builder.method("getPartition", getter(cast(arg(0), recordClass), partitioningKey));
		return builder.newInstance();
	}

	/**
	 * Provides a {@link StreamConsumer} for streaming data to this aggregation.
	 *
	 * @param inputClass class of input records
	 * @param <T>        data records type
	 * @return consumer for streaming data to aggregation
	 */
	public <T> StreamConsumer<T> consumer(Class<T> inputClass) {
		return consumer(inputClass, (List) null, null, new AggregationCommitCallback(this));
	}

	public <T> StreamConsumer<T> consumer(Class<T> inputClass, List<String> fields,
	                                      Map<String, String> outputToInputFields) {
		return consumer(inputClass, fields, outputToInputFields, new AggregationCommitCallback(this));
	}

	/**
	 * Provides a {@link StreamConsumer} for streaming data to this aggregation.
	 *
	 * @param inputClass          class of input records
	 * @param fields              list of output field names
	 * @param outputToInputFields mapping from output to input fields
	 * @param chunksCallback      callback which is called when chunks are created
	 * @param <T>                 data records type
	 * @return consumer for streaming data to aggregation
	 */
	@SuppressWarnings("unchecked")
	public <T> StreamConsumer<T> consumer(Class<T> inputClass, List<String> fields, Map<String, String> outputToInputFields,
	                                      ResultCallback<List<AggregationChunk.NewChunk>> chunksCallback) {
		logger.info("Started consuming data in aggregation {}. Fields: {}. Output to input fields mapping: {}",
				this, fields, outputToInputFields);

		List<String> outputFields = fields == null ? getFields() : fields;

		Class<?> keyClass = structure.createKeyClass(getKeys());
		Class<?> aggregationClass = structure.createRecordClass(getKeys(), outputFields);

		Function<T, Comparable<?>> keyFunction = structure.createKeyFunction(inputClass, keyClass, getKeys());

		Aggregate aggregate = processorFactory.createPreaggregator(inputClass, aggregationClass, getKeys(),
				fields, outputToInputFields);

		PartitioningStrategy partitioningStrategy = createPartitioningStrategy(aggregationClass);

		return new AggregationGroupReducer<>(eventloop, aggregationChunkStorage, metadataStorage, getKeys(),
				outputFields, partitioningStrategy, aggregationClass, keyFunction, aggregate, chunksCallback,
				aggregationChunkSize);
	}

	/**
	 * Returns a {@link StreamProducer} of the records retrieved from aggregation for the specified query.
	 *
	 * @param <T>         type of output objects
	 * @param query       query
	 * @param outputClass class of output records
	 * @return producer that streams query results
	 */
	@SuppressWarnings("unchecked")
	public <T> StreamProducer<T> query(AggregationQuery query, List<String> fields, Class<T> outputClass) {
		List<String> resultKeys = query.getResultKeys();

		List<String> aggregationFields = getAggregationFieldsForQuery(fields);

		List<AggregationChunk> allChunks = aggregationMetadata.findChunks(structure, query.getPredicates(), aggregationFields);

		AggregationQueryPlan queryPlan = new AggregationQueryPlan();

		StreamProducer streamProducer = consolidatedProducer(query.getAllKeys(), aggregationFields,
				outputClass, query.getPredicates(), allChunks, queryPlan);

		StreamProducer queryResultProducer = streamProducer;

		List<AggregationQuery.PredicateNotEquals> notEqualsPredicates = getNotEqualsPredicates(query.getPredicates());

		for (String key : resultKeys) {
			Object restrictedValue = structure.getKeyType(key).getRestrictedValue();
			if (restrictedValue != null)
				notEqualsPredicates.add(new AggregationQuery.PredicateNotEquals(key, restrictedValue));
		}

		if (!notEqualsPredicates.isEmpty()) {
			StreamFilter streamFilter = new StreamFilter<>(eventloop, createNotEqualsPredicate(outputClass, notEqualsPredicates));
			streamProducer.streamTo(streamFilter.getInput());
			queryResultProducer = streamFilter.getOutput();
			queryPlan.setPostFiltering(true);
		}

		if (sortingRequired(resultKeys, getKeys())) {
			Comparator keyComparator = structure.createKeyComparator(outputClass, resultKeys);
			Path path = Paths.get("sorterStorage", "%d.part");
			BufferSerializer bufferSerializer = structure.createBufferSerializer(outputClass, getKeys(), aggregationFields);
			StreamMergeSorterStorage sorterStorage = new StreamMergeSorterStorageImpl(eventloop, executorService,
					bufferSerializer, path, sorterBlockSize);
			StreamSorter sorter = new StreamSorter(eventloop, sorterStorage, Functions.identity(), keyComparator, false,
					sorterItemsInMemory);
			queryResultProducer.streamTo(sorter.getInput());
			queryResultProducer = sorter.getOutput();
			queryPlan.setAdditionalSorting(true);
		}

		logger.info("Query plan for {} in aggregation {}: {}", query, this, queryPlan);

		return queryResultProducer;
	}

	public <T> StreamProducer<T> query(AggregationQuery query, Class<T> outputClass) {
		return query(query, query.getResultFields(), outputClass);
	}

	private List<AggregationQuery.PredicateNotEquals> getNotEqualsPredicates(AggregationQuery.Predicates queryPredicates) {
		List<AggregationQuery.PredicateNotEquals> notEqualsPredicates = newArrayList();

		for (AggregationQuery.Predicate queryPredicate : queryPredicates.asCollection()) {
			if (queryPredicate instanceof AggregationQuery.PredicateNotEquals) {
				notEqualsPredicates.add((AggregationQuery.PredicateNotEquals) queryPredicate);
			}
		}

		return notEqualsPredicates;
	}

	private boolean sortingRequired(List<String> resultKeys, List<String> aggregationKeys) {
		boolean resultKeysAreSubset = !all(aggregationKeys, in(resultKeys));
		return resultKeysAreSubset && !isPrefix(resultKeys, aggregationKeys);
	}

	private boolean isPrefix(List<String> fields1, List<String> fields2) {
		checkArgument(fields1.size() <= fields2.size());
		for (int i = 0; i < fields1.size(); ++i) {
			String resultKey = fields1.get(i);
			String aggregationKey = fields2.get(i);
			if (!resultKey.equals(aggregationKey)) {
				// not prefix
				return false;
			}
		}
		return true;
	}

	private void doConsolidation(final List<AggregationChunk> chunksToConsolidate,
	                             final ResultCallback<List<AggregationChunk.NewChunk>> callback) {
		List<String> fields = new ArrayList<>();
		for (AggregationChunk chunk : chunksToConsolidate) {
			for (String field : chunk.getFields()) {
				if (!fields.contains(field) && getFields().contains(field)) {
					fields.add(field);
				}
			}
		}

		Class resultClass = structure.createRecordClass(getKeys(), fields);

		PartitioningStrategy partitioningStrategy = createPartitioningStrategy(resultClass);

		consolidatedProducer(getKeys(), fields, resultClass, null, chunksToConsolidate, null)
				.streamTo(createChunker(partitioningStrategy, eventloop, getKeys(), fields, resultClass,
						aggregationChunkStorage, metadataStorage, aggregationChunkSize, callback));
	}

	private <T> StreamProducer<T> consolidatedProducer(List<String> keys, List<String> fields, Class<T> resultClass,
	                                                   AggregationQuery.Predicates predicates,
	                                                   List<AggregationChunk> individualChunks,
	                                                   AggregationQueryPlan queryPlan) {
		Set<String> fieldsSet = newHashSet(fields);
		individualChunks = newArrayList(individualChunks);
		Collections.sort(individualChunks, new Comparator<AggregationChunk>() {
			@Override
			public int compare(AggregationChunk chunk1, AggregationChunk chunk2) {
				return chunk1.getMinPrimaryKey().compareTo(chunk2.getMinPrimaryKey());
			}
		});

		List<StreamProducer> producers = new ArrayList<>();
		List<List<String>> producersFields = new ArrayList<>();
		List<Class<?>> producersClasses = new ArrayList<>();

		List<AggregationChunk> chunks = new ArrayList<>();

		/*
		Build producers. Chunks can be read sequentially (using StreamProducers.concat) when the ranges of their keys do not overlap.
		 */
		for (int i = 0; i <= individualChunks.size(); i++) {
			AggregationChunk chunk = (i != individualChunks.size()) ? individualChunks.get(i) : null;

			boolean nextSequence = chunks.isEmpty() || chunk == null ||
					getLast(chunks).getMaxPrimaryKey().compareTo(chunk.getMinPrimaryKey()) >= 0 ||
					!newHashSet(getLast(chunks).getFields()).equals(newHashSet(chunk.getFields()));

			if (nextSequence && !chunks.isEmpty()) {
				List<String> sequenceFields = chunks.get(0).getFields();
				Set<String> requestedFieldsInSequence = intersection(fieldsSet, newLinkedHashSet(sequenceFields));

				Class<?> chunksClass = structure.createRecordClass(getKeys(), sequenceFields);

				producersFields.add(sequenceFields);
				producersClasses.add(chunksClass);

				List<AggregationChunk> sequentialChunkGroup = newArrayList(chunks);

				if (queryPlan != null) {
					queryPlan.addChunkGroup(newArrayList(requestedFieldsInSequence), sequentialChunkGroup);
				}

				StreamProducer producer = sequentialProducer(predicates, sequentialChunkGroup, chunksClass);
				producers.add(producer);

				chunks.clear();
			}

			if (chunk != null) {
				chunks.add(chunk);
			}
		}

		return mergeProducers(keys, fields, resultClass, producers, producersFields, producersClasses);
	}

	private <T> StreamProducer<T> mergeProducers(List<String> keys, List<String> fields, Class<?> resultClass,
	                                             List<StreamProducer> producers, List<List<String>> producersFields,
	                                             List<Class<?>> producerClasses) {
		StreamReducer<Comparable, T, Object> streamReducer = new StreamReducer<>(eventloop, Ordering.natural());

		Class<?> keyClass = structure.createKeyClass(keys);

		for (int i = 0; i < producers.size(); i++) {
			StreamProducer producer = producers.get(i);

			Function extractKeyFunction = structure.createKeyFunction(producerClasses.get(i), keyClass, keys);

			StreamReducers.Reducer reducer = processorFactory.aggregationReducer(producerClasses.get(i), resultClass,
					keys, newArrayList(filter(fields, in(producersFields.get(i)))));

			producer.streamTo(streamReducer.newInput(extractKeyFunction, reducer));
		}
		return streamReducer.getOutput();
	}

	private StreamProducer sequentialProducer(final AggregationQuery.Predicates predicates,
	                                          List<AggregationChunk> individualChunks, final Class<?> sequenceClass) {
		checkArgument(!individualChunks.isEmpty());
		AsyncIterator<StreamProducer<Object>> producerAsyncIterator = AsyncIterators.transform(individualChunks.iterator(),
				new AsyncFunction<AggregationChunk, StreamProducer<Object>>() {
					@Override
					public void apply(AggregationChunk chunk, ResultCallback<StreamProducer<Object>> producerCallback) {
						producerCallback.onResult(chunkReaderWithFilter(predicates, chunk, sequenceClass));
					}
				});
		return StreamProducers.concat(eventloop, producerAsyncIterator);
	}

	private StreamProducer chunkReaderWithFilter(AggregationQuery.Predicates predicates,
	                                             AggregationChunk chunk, Class<?> chunkRecordClass) {
		StreamProducer chunkReader = aggregationChunkStorage.chunkReader(getKeys(),
				chunk.getFields(), chunkRecordClass, chunk.getChunkId());
		StreamProducer chunkProducer = chunkReader;
		if (ignoreChunkReadingExceptions) {
			ErrorIgnoringTransformer errorIgnoringTransformer = new ErrorIgnoringTransformer<>(eventloop);
			chunkReader.streamTo(errorIgnoringTransformer.getInput());
			chunkProducer = errorIgnoringTransformer.getOutput();
		}
		if (predicates == null)
			return chunkProducer;
		StreamFilter streamFilter = new StreamFilter<>(eventloop,
				createPredicate(chunk, chunkRecordClass, predicates));
		chunkProducer.streamTo(streamFilter.getInput());
		return streamFilter.getOutput();
	}

	private Predicate createNotEqualsPredicate(Class<?> recordClass, List<AggregationQuery.PredicateNotEquals> notEqualsPredicates) {
		AsmBuilder<Predicate> builder = new AsmBuilder<>(classLoader, Predicate.class);
		PredicateDefAnd predicateDefAnd = and();
		for (AggregationQuery.PredicateNotEquals notEqualsPredicate : notEqualsPredicates) {
			predicateDefAnd.add(cmpNe(
					getter(cast(arg(0), recordClass), notEqualsPredicate.key),
					value(notEqualsPredicate.value)
			));
		}
		builder.method("apply", boolean.class, asList(Object.class), predicateDefAnd);
		return builder.newInstance();
	}

	private Predicate createPredicate(AggregationChunk chunk,
	                                  Class<?> chunkRecordClass, AggregationQuery.Predicates predicates) {
		List<String> keysAlreadyInChunk = new ArrayList<>();
		for (int i = 0; i < getKeys().size(); i++) {
			String key = getKeys().get(i);
			Object min = chunk.getMinPrimaryKey().get(i);
			Object max = chunk.getMaxPrimaryKey().get(i);
			if (!min.equals(max)) {
				break;
			}
			keysAlreadyInChunk.add(key);
		}

		AsmBuilder builder = new AsmBuilder(classLoader, Predicate.class);
		PredicateDefAnd predicateDefAnd = and();

		for (AggregationQuery.Predicate predicate : predicates.asCollection()) {
			if (predicate instanceof AggregationQuery.PredicateEq) {
//				if (keysAlreadyInChunk.contains(predicate.key))
//					continue;
				Object value = ((AggregationQuery.PredicateEq) predicate).value;

				predicateDefAnd.add(cmpEq(
						getter(cast(arg(0), chunkRecordClass), predicate.key),
						value(value)));
			} else if (predicate instanceof AggregationQuery.PredicateBetween) {
				Object from = ((AggregationQuery.PredicateBetween) predicate).from;
				Object to = ((AggregationQuery.PredicateBetween) predicate).to;

				predicateDefAnd.add(cmpGe(
						getter(cast(arg(0), chunkRecordClass), predicate.key),
						value(from)));

				predicateDefAnd.add(cmpLe(
						getter(cast(arg(0), chunkRecordClass), predicate.key),
						value(to)));
			}
		}
		builder.method("apply", boolean.class, asList(Object.class), predicateDefAnd);
		return (Predicate) builder.newInstance();
	}

	public void consolidate(int maxChunksToConsolidate,
	                        final ResultCallback<Boolean> callback) {
		logger.trace("Aggregation {} consolidation started", this);

		final List<AggregationChunk> chunksToConsolidate;
		List<AggregationChunk> foundChunksToConsolidate = aggregationMetadata.findChunksToConsolidate();
		if (foundChunksToConsolidate.size() <= maxChunksToConsolidate) {
			chunksToConsolidate = foundChunksToConsolidate;
		} else {
			List<AggregationChunk> chunks = new ArrayList(foundChunksToConsolidate);
			Collections.sort(chunks, new Comparator<AggregationChunk>() {
				@Override
				public int compare(AggregationChunk chunk1, AggregationChunk chunk2) {
					return Integer.compare(chunk1.getCount(), chunk2.getCount());
				}
			});
			chunksToConsolidate = chunks.subList(0, maxChunksToConsolidate);
		}
		if (chunksToConsolidate.isEmpty()) {
			callback.onResult(false);
			return;
		}

		logger.info("Starting consolidation of the following chunks in aggregation '{}': [{}]",
				aggregationMetadata, getChunkIds(chunksToConsolidate));
		metadataStorage.startConsolidation(chunksToConsolidate, new ForwardingCompletionCallback(callback) {
			@Override
			public void onComplete() {
				doConsolidation(chunksToConsolidate, new ForwardingResultCallback<List<AggregationChunk.NewChunk>>(callback) {
					@Override
					public void onResult(final List<AggregationChunk.NewChunk> consolidatedChunks) {
						metadataStorage.saveConsolidatedChunks(chunksToConsolidate,
								consolidatedChunks, new ForwardingCompletionCallback(callback) {
									@Override
									public void onComplete() {
										logger.info("Completed consolidation of the following chunks " +
														"in aggregation '{}': [{}]. Created chunks: [{}]",
												aggregationMetadata, getChunkIds(chunksToConsolidate),
												getNewChunkIds(consolidatedChunks));
										callback.onResult(true);
									}
								});
					}
				});
			}
		});
	}

	private static String getNewChunkIds(List<AggregationChunk.NewChunk> chunks) {
		List<Long> ids = new ArrayList<>();
		for (AggregationChunk.NewChunk chunk : chunks) {
			ids.add(chunk.chunkId);
		}

		return JOINER.join(ids);
	}

	private static String getChunkIds(List<AggregationChunk> chunks) {
		List<Long> ids = new ArrayList<>();
		for (AggregationChunk chunk : chunks) {
			ids.add(chunk.getChunkId());
		}

		return JOINER.join(ids);
	}

	public void loadChunks(final CompletionCallback callback) {
		if (loadChunksCallback != null) {
			logger.info("Loading chunks for aggregation {} is already started. Added callback", this);
			loadChunksCallback.addCallback(callback);
			return;
		}

		logger.info("Loading chunks for aggregation {}", this);
		loadChunksCallback = new ListenableCompletionCallback();
		loadChunksCallback.addCallback(callback);
		metadataStorage.loadChunks(lastRevisionId, new ResultCallback<LoadedChunks>() {
			@Override
			public void onResult(LoadedChunks loadedChunks) {
				for (AggregationChunk newChunk : loadedChunks.newChunks) {
					addToIndex(newChunk);
					logger.trace("Added chunk {} to index", newChunk);
				}
				for (Long consolidatedChunkId : loadedChunks.consolidatedChunkIds) {
					AggregationChunk chunk = chunks.get(consolidatedChunkId);
					if (chunk != null) {
						removeFromIndex(chunk);
						logger.trace("Removed chunk {} from index", chunk);
					}
				}
				Aggregation.this.lastRevisionId = loadedChunks.lastRevisionId;
				logger.info("Loading chunks for aggregation {} completed. " +
						"Loaded {} new chunks and {} consolidated chunks. Revision id: {}",
						Aggregation.this, loadedChunks.newChunks.size(), loadedChunks.consolidatedChunkIds.size(),
						loadedChunks.lastRevisionId);

				loadChunksCallback.onComplete();
				loadChunksCallback = null;
			}

			@Override
			public void onException(Exception exception) {
				logger.error("Loading chunks for aggregation {} failed", this, exception);
				loadChunksCallback.onException(exception);
				loadChunksCallback = null;
			}
		});
	}

	public int getAggregationChunkSize() {
		return aggregationChunkSize;
	}

	public void setAggregationChunkSize(int aggregationChunkSize) {
		this.aggregationChunkSize = aggregationChunkSize;
	}

	public int getSorterItemsInMemory() {
		return sorterItemsInMemory;
	}

	public void setSorterItemsInMemory(int sorterItemsInMemory) {
		this.sorterItemsInMemory = sorterItemsInMemory;
	}

	public int getSorterBlockSize() {
		return sorterBlockSize;
	}

	public void setSorterBlockSize(int sorterBlockSize) {
		this.sorterBlockSize = sorterBlockSize;
	}

	public boolean isIgnoreChunkReadingExceptions() {
		return ignoreChunkReadingExceptions;
	}

	public void setIgnoreChunkReadingExceptions(boolean ignoreChunkReadingExceptions) {
		this.ignoreChunkReadingExceptions = ignoreChunkReadingExceptions;
	}

	@Override
	public String toString() {
		return getKeys().toString();
	}
}

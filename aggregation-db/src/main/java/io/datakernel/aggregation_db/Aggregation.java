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

import com.google.common.base.*;
import com.google.common.collect.Ordering;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ForwardingCompletionCallback;
import io.datakernel.async.ForwardingResultCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.codegen.AsmBuilder;
import io.datakernel.codegen.PredicateDefAnd;
import io.datakernel.codegen.utils.DefiningClassLoader;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;
import io.datakernel.stream.processor.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Predicates.in;
import static com.google.common.collect.Iterables.*;
import static com.google.common.collect.Lists.newArrayList;
import static io.datakernel.codegen.Expressions.*;
import static java.util.Arrays.asList;

/**
 * Represents an aggregation, which aggregates data using custom reducer and preaggregator.
 * Provides methods for loading and querying data.
 */
@SuppressWarnings("unchecked")
public class Aggregation {
	private static final Logger logger = LoggerFactory.getLogger(Aggregation.class);

	private final Eventloop eventloop;
	private final DefiningClassLoader classLoader;
	private final AggregationMetadataStorage metadataStorage;
	private final AggregationChunkStorage aggregationChunkStorage;
	private final AggregationMetadata aggregationMetadata;
	private final int aggregationChunkSize;
	private final int sorterItemsInMemory;
	private final int consolidationTimeoutMillis;
	private final int removeChunksAfterConsolidationMillis;

	private final AggregationStructure structure;

	private final ProcessorFactory processorFactory;

	private final Map<Long, AggregationChunk> chunks = new LinkedHashMap<>();

	private int lastRevisionId;

	/**
	 * Instantiates an aggregation with the specified structure, that runs in a given event loop,
	 * uses the specified class loader for creating dynamic classes, saves data and metadata to given storages,
	 * and uses the specified parameters.
	 *
	 * @param eventloop                            event loop, in which the aggregation is to run
	 * @param classLoader                          class loader for defining dynamic classes
	 * @param metadataStorage                      storage for aggregations metadata
	 * @param aggregationChunkStorage              storage for data chunks
	 * @param aggregationMetadata                  metadata of the aggregation
	 * @param structure                            structure of an aggregation
	 * @param processorFactory                     factory used to instantiate reducer and preaggregators
	 * @param aggregationChunkSize                 maximum size of aggregation chunk
	 * @param sorterItemsInMemory                  maximum number of records that can stay in memory while sorting
	 * @param consolidationTimeoutMillis           maximum duration of consolidation attempt (in milliseconds)
	 * @param removeChunksAfterConsolidationMillis period of time (in milliseconds) after consolidation after which consolidated chunks can be removed
	 */
	public Aggregation(Eventloop eventloop, DefiningClassLoader classLoader, AggregationMetadataStorage metadataStorage,
	                   AggregationChunkStorage aggregationChunkStorage, AggregationMetadata aggregationMetadata, AggregationStructure structure,
	                   ProcessorFactory processorFactory, int sorterItemsInMemory,
	                   int aggregationChunkSize, int consolidationTimeoutMillis, int removeChunksAfterConsolidationMillis) {
		this.eventloop = eventloop;
		this.classLoader = classLoader;
		this.metadataStorage = metadataStorage;
		this.aggregationChunkStorage = aggregationChunkStorage;
		this.aggregationMetadata = aggregationMetadata;
		this.sorterItemsInMemory = sorterItemsInMemory;
		this.aggregationChunkSize = aggregationChunkSize;
		this.structure = structure;
		this.processorFactory = processorFactory;
		this.consolidationTimeoutMillis = consolidationTimeoutMillis;
		this.removeChunksAfterConsolidationMillis = removeChunksAfterConsolidationMillis;
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
	 * @param processorFactory        factory used to instantiate reducer and preaggregators
	 */
	public Aggregation(Eventloop eventloop, DefiningClassLoader classLoader, AggregationMetadataStorage metadataStorage,
	                   AggregationChunkStorage aggregationChunkStorage, AggregationMetadata aggregationMetadata, AggregationStructure structure,
	                   ProcessorFactory processorFactory) {
		this(eventloop, classLoader, metadataStorage, aggregationChunkStorage, aggregationMetadata, structure, processorFactory,
				1_000_000, 1_000_000, 30 * 60 * 1000, 10 * 60 * 1000);
	}

	public List<String> getAggregationFieldsForConsumer(List<String> fields) {
		return newArrayList(filter(aggregationMetadata.getInputFields(), in(fields)));
	}

	public List<String> getAggregationFieldsForQuery(List<String> queryFields) {
		return newArrayList(filter(queryFields, in(aggregationMetadata.getInputFields())));
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

	public void addChunk(AggregationChunk chunk) {
		chunks.put(chunk.getChunkId(), chunk);
	}

	public AggregationMetadata getAggregationMetadata() {
		return aggregationMetadata;
	}

	public void addToIndex(AggregationChunk chunk) {
		aggregationMetadata.addToIndex(chunk);
		addChunk(chunk);
	}

	public boolean hasPredicates() {
		return aggregationMetadata.hasPredicates();
	}

	public boolean matchQueryPredicates(AggregationQuery.QueryPredicates predicates) {
		return aggregationMetadata.matchQueryPredicates(predicates);
	}

	public AggregationFilteringResult applyQueryPredicates(AggregationQuery query, AggregationStructure structure) {
		return aggregationMetadata.applyQueryPredicates(query, structure);
	}

	public void removeFromIndex(AggregationChunk chunk) {
		aggregationMetadata.removeFromIndex(chunk);
	}

	public List<String> getKeys() {
		return aggregationMetadata.getKeys();
	}

	public int getNumberOfPredicates() {
		return aggregationMetadata.getNumberOfPredicates();
	}

	public List<String> getInputFields() {
		return aggregationMetadata.getInputFields();
	}

	public List<String> getOutputFields() {
		return aggregationMetadata.getOutputFields();
	}

	public String getId() {
		return aggregationMetadata.getId();
	}

	public double getCost(AggregationQuery query) {
		return aggregationMetadata.getCost(query);
	}

	public AggregationQuery.QueryPredicates getAggregationPredicates() {
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

	/**
	 * Provides a {@link StreamConsumer} for streaming data to this aggregation.
	 *
	 * @param inputClass class of input records
	 * @param <T>        data records type
	 * @return consumer for streaming data to aggregation
	 */
	public <T> StreamConsumer<T> consumer(Class<T> inputClass) {
		return consumer(inputClass, new AggregationCommitCallback(this));
	}

	/**
	 * Provides a {@link StreamConsumer} for streaming data to this aggregation.
	 *
	 * @param inputClass     class of input records
	 * @param chunksCallback callback which is called when chunks are created
	 * @param <T>            data records type
	 * @return consumer for streaming data to aggregation
	 */
	public <T> StreamConsumer<T> consumer(Class<T> inputClass, ResultCallback<List<AggregationChunk.NewChunk>> chunksCallback) {
		return consumer(inputClass, null, chunksCallback);
	}

	/**
	 * Provides a {@link StreamConsumer} for streaming data to this aggregation.
	 *
	 * @param inputClass     class of input records
	 * @param inputFields    list of input field names
	 * @param chunksCallback callback which is called when chunks are created
	 * @param <T>            data records type
	 * @return consumer for streaming data to aggregation
	 */
	@SuppressWarnings("unchecked")
	public <T> StreamConsumer<T> consumer(Class<T> inputClass, List<String> inputFields,
	                                      ResultCallback<List<AggregationChunk.NewChunk>> chunksCallback) {
		if (inputFields == null)
			inputFields = aggregationMetadata.getInputFields();

		Class<?> keyClass = structure.createKeyClass(aggregationMetadata.getKeys());
		Class<?> aggregationClass = structure.createRecordClass(aggregationMetadata.getKeys(), aggregationMetadata.getOutputFields());

		Function<T, Comparable<?>> keyFunction = structure.createKeyFunction(inputClass, keyClass,
				aggregationMetadata.getKeys());

		Aggregate aggregate = processorFactory.createPreaggregator(inputClass, aggregationClass, aggregationMetadata.getKeys(),
				inputFields, aggregationMetadata.getOutputFields());

		return new AggregationGroupReducer<>(eventloop, aggregationChunkStorage,
				metadataStorage, aggregationMetadata, inputFields, aggregationClass, keyFunction, aggregate,
				chunksCallback, aggregationChunkSize);
	}

	/**
	 * Returns a {@link StreamProducer} of the records retrieved from aggregation for the specified query.
	 *
	 * @param revisionId  minimum revision of records
	 * @param query       query
	 * @param outputClass class of output records
	 * @param <T>         type of output objects
	 * @return producer that streams query results
	 */
	@SuppressWarnings("unchecked")
	public <T> StreamProducer<T> query(int revisionId, AggregationQuery query, Class<T> outputClass) {
		List<String> resultKeys = query.getResultKeys();

		Class resultKeyClass = structure.createKeyClass(resultKeys);

		List<String> aggregationFields = getAggregationFieldsForQuery(query.getResultFields());

		List<AggregationChunk> allChunks = aggregationMetadata.queryByPredicates(structure, chunks, revisionId, query.getPredicates());

		Function keyFunction = structure.createKeyFunction(outputClass, resultKeyClass, resultKeys);

		StreamProducer streamProducer = consolidatedProducer(query.getAllKeys(), aggregationFields,
				outputClass, query.getPredicates(), allChunks);

		if (sortingRequired(resultKeys, aggregationMetadata.getKeys())) {
			StreamMergeSorterStorage sorterStorage = SorterStorageUtils.getSorterStorage(eventloop, structure,
					outputClass, aggregationMetadata.getKeys(), aggregationFields);
			StreamSorter sorter = new StreamSorter(eventloop, sorterStorage, keyFunction, Ordering.natural(), false,
					sorterItemsInMemory);
			streamProducer.streamTo(sorter);
			return sorter.getSortedStream();
		} else {
			return streamProducer;
		}
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

	public List<AggregationChunk> initiateConsolidation(List<AggregationChunk> foundChunksToConsolidate,
	                                                    final ConsolidateCallback callback) {
		final List<String> fields = new ArrayList<>();
		for (AggregationChunk chunk : foundChunksToConsolidate) {
			for (String field : chunk.getFields()) {
				if (!fields.contains(field)) {
					fields.add(field);
				}
			}
		}

		final Class<?> resultClass = structure.createRecordClass(getKeys(), fields);

		final AggregationMetadata finalAggregation = aggregationMetadata;
		final List<AggregationChunk> finalChunksToConsolidate = foundChunksToConsolidate;

		eventloop.postConcurrently(new Runnable() {
			@Override
			public void run() {
				consolidatedProducer(getKeys(), fields, resultClass,
						null, finalChunksToConsolidate)
						.streamTo(new AggregationChunker(eventloop, getId(), getKeys(), fields, resultClass, aggregationChunkStorage, metadataStorage,
								new ForwardingResultCallback<List<AggregationChunk.NewChunk>>(callback) {
									@Override
									public void onResult(List<AggregationChunk.NewChunk> consolidatedChunks) {
										callback.onConsolidate(finalAggregation,
												finalChunksToConsolidate, consolidatedChunks);
									}
								}, aggregationChunkSize));
			}
		});

		return finalChunksToConsolidate;
	}

	private <T> StreamProducer<T> consolidatedProducer(List<String> keys, List<String> fields, Class<T> resultClass,
	                                                   AggregationQuery.QueryPredicates predicates,
	                                                   List<AggregationChunk> individualChunks) {
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
					!getLast(chunks).getFields().equals(chunk.getFields());

			if (nextSequence && !chunks.isEmpty()) {
				Class<?> chunksClass = structure.createRecordClass(aggregationMetadata.getKeys(),
						chunks.get(0).getFields());

				producersFields.add(chunks.get(0).getFields());
				producersClasses.add(chunksClass);

				StreamProducer producer = sequentialProducer(aggregationMetadata, predicates, chunks, chunksClass);
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
					keys, newArrayList(filter(fields, in(producersFields.get(i)))), aggregationMetadata.getOutputFields());

			producer.streamTo(streamReducer.newInput(extractKeyFunction, reducer));
		}
		return streamReducer;
	}

	private StreamProducer sequentialProducer(AggregationMetadata aggregation, AggregationQuery.QueryPredicates predicates,
	                                          List<AggregationChunk> individualChunks, Class<?> sequenceClass) {
		checkArgument(!individualChunks.isEmpty());
		List<StreamProducer<Object>> producers = new ArrayList<>();
		for (AggregationChunk chunk : individualChunks) {
			producers.add(chunkReaderWithFilter(aggregation, predicates, chunk, sequenceClass));
		}
		return StreamProducers.concat(eventloop, producers);
	}

	private StreamProducer chunkReaderWithFilter(AggregationMetadata aggregationMetadata, AggregationQuery.QueryPredicates predicates,
	                                             AggregationChunk chunk, Class<?> chunkRecordClass) {
		StreamProducer chunkReader = aggregationChunkStorage.chunkReader(aggregationMetadata.getId(), aggregationMetadata.getKeys(),
				chunk.getFields(), chunkRecordClass, chunk.getChunkId());
		if (predicates == null)
			return chunkReader;
		StreamFilter streamFilter = new StreamFilter<>(eventloop,
				createPredicate(aggregationMetadata, chunk, chunkRecordClass, predicates));
		chunkReader.streamTo(streamFilter);
		return streamFilter;
	}

	private Predicate createPredicate(AggregationMetadata aggregationMetadata, AggregationChunk chunk,
	                                  Class<?> chunkRecordClass, AggregationQuery.QueryPredicates predicates) {
		List<String> keysAlreadyInChunk = new ArrayList<>();
		for (int i = 0; i < aggregationMetadata.getKeys().size(); i++) {
			String key = aggregationMetadata.getKeys().get(i);
			Object min = chunk.getMinPrimaryKey().get(i);
			Object max = chunk.getMaxPrimaryKey().get(i);
			if (!min.equals(max)) {
				break;
			}
			keysAlreadyInChunk.add(key);
		}

		AsmBuilder builder = new AsmBuilder(classLoader, Predicate.class);
		PredicateDefAnd predicateDefAnd = and();

		for (AggregationQuery.QueryPredicate predicate : predicates.asCollection()) {
			if (keysAlreadyInChunk.contains(predicate.key))
				continue;
			if (predicate instanceof AggregationQuery.QueryPredicateEq) {
				Object value = ((AggregationQuery.QueryPredicateEq) predicate).value;

				predicateDefAnd.add(cmpEq(
						field(cast(arg(0), chunkRecordClass), predicate.key),
						value(value)));
			} else if (predicate instanceof AggregationQuery.QueryPredicateBetween) {
				Object from = ((AggregationQuery.QueryPredicateBetween) predicate).from;
				Object to = ((AggregationQuery.QueryPredicateBetween) predicate).to;

				predicateDefAnd.add(cmpGe(
						field(cast(arg(0), chunkRecordClass), predicate.key),
						value(from)));

				predicateDefAnd.add(cmpLe(
						field(cast(arg(0), chunkRecordClass), predicate.key),
						value(to)));
			} else {
				throw new IllegalArgumentException("Unsupported predicate " + predicate);
			}
		}
		builder.method("apply", boolean.class, asList(Object.class), predicateDefAnd);
		return (Predicate) builder.newInstance();
	}

	public void consolidate(final CompletionCallback callback) {
		consolidate(new ForwardingConsolidateCallback(callback) {
			@Override
			public void onConsolidate(AggregationMetadata aggregationMetadata,
			                          List<AggregationChunk> originalChunks,
			                          List<AggregationChunk.NewChunk> consolidatedChunks) {
				metadataStorage.saveConsolidatedChunks(Aggregation.this, aggregationMetadata, originalChunks, consolidatedChunks, new ForwardingCompletionCallback(callback) {
					@Override
					public void onComplete() {
						callback.onComplete();
					}
				});
			}

			@Override
			public void onNothingToConsolidate() {
				callback.onComplete();
			}
		});
	}

	public int getNumberOfChunksAvailableForConsolidation() {
		return aggregationMetadata.findChunksToConsolidate(newArrayList(chunks.keySet())).size();
	}

	@SuppressWarnings("unchecked")
	public void consolidate(final ConsolidateCallback callback) {
		Function<List<Long>, List<AggregationChunk>> chunkConsolidator = new Function<List<Long>, List<AggregationChunk>>() {
			@Override
			public List<AggregationChunk> apply(List<Long> consolidationCandidateChunkIds) {
				List<AggregationChunk> foundChunksToConsolidate = aggregationMetadata.findChunksToConsolidate(consolidationCandidateChunkIds);

				if (foundChunksToConsolidate.isEmpty()) {
					callback.onNothingToConsolidate();
					return new ArrayList<>();
				}

				initiateConsolidation(foundChunksToConsolidate, callback);

				return foundChunksToConsolidate;
			}
		};

		metadataStorage.performConsolidation(this, chunkConsolidator, new CompletionCallback() {
			@Override
			public void onComplete() {
				logger.trace("Aggregation {} consolidation successfully started.", this);
			}

			@Override
			public void onException(Exception exception) {
				logger.error("Aggregation {} consolidation failed to start.", this, exception);
			}
		});
	}

	public List<Long> getIdsOfNotConsolidatedChunks() {
		List<Long> ids = newArrayList();

		for (Map.Entry<Long, AggregationChunk> chunkEntry : chunks.entrySet()) {
			if (!chunkEntry.getValue().isConsolidated()) {
				ids.add(chunkEntry.getKey());
			}
		}

		return ids;
	}

	public List<Long> getIdsOfChunksAvailableForConsolidation() {
		List<Long> ids = newArrayList();

		for (Map.Entry<Long, AggregationChunk> chunkEntry : chunks.entrySet()) {
			AggregationChunk chunk = chunkEntry.getValue();
			Long chunkId = chunkEntry.getKey();
			boolean chunkIsConsolidated = chunk.isConsolidated();

			if (chunkIsConsolidated) {
				continue;
			}

			Timestamp consolidationStarted = chunk.getConsolidationStarted();

			if (consolidationStarted == null) {
				ids.add(chunkId);
				continue;
			}

			long consolidationTimeoutTimestamp = consolidationStarted.getTime() + consolidationTimeoutMillis;
			long currentTimestamp = eventloop.currentTimeMillis();

			if (currentTimestamp > consolidationTimeoutTimestamp) {
				ids.add(chunkId);
			}
		}

		return ids;
	}

	public void removeOldChunks(final CompletionCallback completionCallback) {
		for (Map.Entry<Long, AggregationChunk> chunkEntry : chunks.entrySet()) {
			final AggregationChunk chunk = chunkEntry.getValue();
			if (chunk.isConsolidated()) {
				long currentTimestamp = eventloop.currentTimeMillis();
				long chunkRemovalTimestamp = chunk.getConsolidationCompleted().getTime() + removeChunksAfterConsolidationMillis;

				if (currentTimestamp > chunkRemovalTimestamp) {
					final long chunkId = chunk.getChunkId();
					final String aggregationId = chunk.getAggregationId();
					metadataStorage.removeChunk(chunkId, new CompletionCallback() {
						@Override
						public void onComplete() {
							removeFromIndex(chunk);
							aggregationChunkStorage.removeChunk(aggregationId, chunkId);
							logger.info("Removed chunk #{}", chunkId);
							completionCallback.onComplete();
						}

						@Override
						public void onException(Exception exception) {
							logger.error("Removal of chunk #{} failed.", chunkId, exception);
							completionCallback.onException(exception);
						}
					});
				}
			}
		}
	}

	public void loadChunks(int maxRevisionId, final CompletionCallback callback) {
		metadataStorage.loadChunks(this, lastRevisionId, maxRevisionId, new ForwardingResultCallback<Integer>(callback) {
			@Override
			public void onResult(Integer result) {
				Aggregation.this.lastRevisionId = result;
				callback.onComplete();
			}
		});
	}

	public void loadChunksBlocking(int maxRevisionId) {
		lastRevisionId = metadataStorage.loadChunks(this, lastRevisionId, maxRevisionId);
	}

	public void reloadAllChunksConsolidations(CompletionCallback callback) {
		metadataStorage.reloadAllChunkConsolidations(this, callback);
	}

	public void refreshChunkConsolidations(CompletionCallback callback) {
		metadataStorage.refreshNotConsolidatedChunks(this, callback);
	}

	@Override
	public String toString() {
		return MoreObjects.toStringHelper(this)
				.add("aggregationMetadata", aggregationMetadata)
				.toString();
	}
}
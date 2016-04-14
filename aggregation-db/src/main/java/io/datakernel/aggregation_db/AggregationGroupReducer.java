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
import io.datakernel.aggregation_db.util.AsyncResultsTracker;
import io.datakernel.aggregation_db.util.AsyncResultsTracker.AsyncResultsTrackerList;
import io.datakernel.async.ResultCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.jmx.EventloopJmxMBean;
import io.datakernel.jmx.JmxOperation;
import io.datakernel.stream.AbstractStreamConsumer;
import io.datakernel.stream.StreamDataReceiver;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.google.common.collect.Iterables.transform;

public final class AggregationGroupReducer<T> extends AbstractStreamConsumer<T> implements StreamDataReceiver<T>, EventloopJmxMBean {
	private static final Logger logger = LoggerFactory.getLogger(AggregationGroupReducer.class);

	private static final int MAX_OUTPUT_STREAMS = 1;

	private final AggregationChunkStorage storage;
	private final AggregationMetadataStorage metadataStorage;
	private final List<String> keys;
	private final List<String> fields;
	private final PartitioningStrategy partitioningStrategy;
	private final Class<?> recordClass;
	private final Function<T, Comparable<?>> keyFunction;
	private final Aggregate aggregate;
	private final AsyncResultsTrackerList<AggregationChunk.NewChunk> resultsTracker;
	private final int chunkSize;

	private final HashMap<Comparable<?>, Object> map = new HashMap<>();

	public AggregationGroupReducer(Eventloop eventloop, AggregationChunkStorage storage,
	                               AggregationMetadataStorage metadataStorage, List<String> keys, List<String> fields,
	                               PartitioningStrategy partitioningStrategy, Class<?> recordClass,
	                               Function<T, Comparable<?>> keyFunction, Aggregate aggregate,
	                               ResultCallback<List<AggregationChunk.NewChunk>> chunksCallback, int chunkSize) {
		super(eventloop);
		this.storage = storage;
		this.metadataStorage = metadataStorage;
		this.keys = keys;
		this.fields = fields;
		this.partitioningStrategy = partitioningStrategy;
		this.recordClass = recordClass;
		this.keyFunction = keyFunction;
		this.aggregate = aggregate;
		this.chunkSize = chunkSize;
		this.resultsTracker = AsyncResultsTracker.ofList(chunksCallback);
	}

	@Override
	public StreamDataReceiver<T> getDataReceiver() {
		return this;
	}

	@Override
	public void onData(T item) {
		Comparable<?> key = keyFunction.apply(item);
		Object accumulator = map.get(key);
		if (accumulator != null) {
			aggregate.accumulate(accumulator, item);
		} else {
			accumulator = aggregate.createAccumulator(item);
			map.put(key, accumulator);

			if (map.size() == chunkSize) {
				doFlush();
			}
		}
	}

	@SuppressWarnings("unchecked")
	private void doFlush() {
		if (map.isEmpty())
			return;

		resultsTracker.startOperation();

		if (resultsTracker.getOperationsCount() > MAX_OUTPUT_STREAMS)
			suspend();

		final List<Map.Entry<Comparable<?>, Object>> entryList = new ArrayList<>(map.entrySet());
		map.clear();

		Collections.sort(entryList, new Comparator<Map.Entry<Comparable<?>, Object>>() {
			@Override
			public int compare(Map.Entry<Comparable<?>, Object> o1, Map.Entry<Comparable<?>, Object> o2) {
				Comparable<Object> key1 = (Comparable<Object>) o1.getKey();
				Comparable<Object> key2 = (Comparable<Object>) o2.getKey();
				return key1.compareTo(key2);
			}
		});

		Iterable<Object> list = transform(entryList, new Function<Map.Entry<Comparable<?>, Object>, Object>() {
			@Override
			public Object apply(Map.Entry<Comparable<?>, Object> input) {
				return input.getValue();
			}
		});

		final StreamProducer producer = StreamProducers.ofIterable(eventloop, list);

		producer.streamTo(Aggregation.createChunker(partitioningStrategy, eventloop, keys, fields,
				recordClass, storage, metadataStorage, chunkSize,
				new ResultCallback<List<AggregationChunk.NewChunk>>() {
					@Override
					protected void onResult(List<AggregationChunk.NewChunk> newChunks) {
						resultsTracker.completeWithResults(newChunks);

						if (resultsTracker.getOperationsCount() <= MAX_OUTPUT_STREAMS)
							resume();
					}

					@Override
					protected void onException(Exception e) {
						logger.error("Streaming to chunker failed", e);
						closeWithError(e);
						resultsTracker.completeWithException(e);
					}
				}));
	}

	@Override
	public void onEndOfStream() {
		doFlush();
		resultsTracker.shutDown();
	}

	@Override
	protected void onError(Exception e) {
		resultsTracker.shutDownWithException(e);
	}

	// jmx

	@JmxOperation
	public void flush() {
		doFlush();
	}

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}
}

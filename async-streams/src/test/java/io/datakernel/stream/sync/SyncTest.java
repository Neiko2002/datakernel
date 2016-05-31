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

package io.datakernel.stream.sync;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.CompletionCallbackFuture;
import io.datakernel.async.SimpleResultCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducer;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static io.datakernel.stream.StreamProducers.ofValue;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SyncTest {
	static class Record {
		private String key;
		private long timestamp;
		private long value;

		public Record(String key, long timestamp, long value) {
			this.key = key;
			this.timestamp = timestamp;
			this.value = value;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			Record record = (Record) o;
			return timestamp == record.timestamp &&
					value == record.value &&
					Objects.equals(key, record.key);
		}

		@Override
		public int hashCode() {
			return Objects.hash(key, timestamp, value);
		}
	}

	static class TestPeer implements Peer<Map<String, Record>> {
		private Eventloop eventloop;
		private Map<String, Record> state = new HashMap<>();
		private Merger<Map<String, Record>, Map<String, Record>> merger = new LastWriteWinsMerger();

		public TestPeer(Eventloop eventloop) {
			this.eventloop = eventloop;
		}

		@Override
		public void sync(StreamProducer<Map<String, Record>> producer, final CompletionCallback callback) {
			final StreamConsumers.ToList<Map<String, Record>> listConsumer = StreamConsumers.toList(eventloop);
			producer.streamTo(listConsumer);
			listConsumer.setResultCallback(new SimpleResultCallback<List<Map<String, Record>>>() {
				@Override
				protected void onResultOrException(@Nullable List<Map<String, Record>> result) {
					state = merger.merge(state, result.get(0));
					callback.onComplete();
				}
			});
		}
	}

	static class LastWriteWinsMerger implements Merger<Map<String, Record>, Map<String, Record>> {
		@Override
		public Map<String, Record> merge(Map<String, Record> oldState, Map<String, Record> newState) {
			for (Map.Entry<String, Record> entry : newState.entrySet()) {
				String key = entry.getKey();
				Record oldRecord = oldState.get(key);
				Record newRecord = entry.getValue();
				if (oldRecord == null || newRecord.timestamp > oldRecord.timestamp)
					oldState.put(key, newRecord);
			}
			return oldState;
		}
	}

	@Test
	public void test() throws Exception {
		Eventloop eventloop = new Eventloop();
		TestPeer peer = new TestPeer(eventloop);

		List<Record> records = asList(new Record("a", 1, 10), new Record("b", 1, 20), new Record("c", 1, 30));
		Map<String, Record> initialPeerState = toState(records);
		StreamProducer<Map<String, Record>> initialStateProducer = ofValue(eventloop, initialPeerState);

		CompletionCallbackFuture future = new CompletionCallbackFuture();
		peer.sync(initialStateProducer, future);
		eventloop.run();
		assertTrue(future.isDone());

		assertEquals(initialPeerState, peer.state);

		List<Record> newRecords = asList(new Record("a", 1, 11), new Record("b", 0, 21), new Record("c", 2, 31));
		Map<String, Record> newPeerState = toState(newRecords);
		StreamProducer<Map<String, Record>> newStateProducer = ofValue(eventloop, newPeerState);

		future = new CompletionCallbackFuture();
		peer.sync(newStateProducer, future);
		eventloop.run();
		assertTrue(future.isDone());

		Map<String, Record> resultState = new HashMap<>();
		resultState.put("a", new Record("a", 1, 10));
		resultState.put("b", new Record("b", 1, 20));
		resultState.put("c", new Record("c", 2, 31));
		assertEquals(resultState, peer.state);
	}

	private static Map<String, Record> toState(List<Record> records) {
		Map<String, Record> state = new HashMap<>();
		for (Record record : records) {
			state.put(record.key, record);
		}
		return state;
	}
}

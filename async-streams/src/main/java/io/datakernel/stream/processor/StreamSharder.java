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

package io.datakernel.stream.processor;

import com.google.common.base.Function;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.AbstractStreamTransformer_1_N;
import io.datakernel.stream.StreamDataReceiver;
import io.datakernel.stream.StreamProducer;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * It is {@link AbstractStreamTransformer_1_N} which divides input stream  into groups with some key
 * function, and sends obtained streams to consumers.
 *
 * @param <K> type of result key function
 * @param <T> type of input items
 */
@SuppressWarnings("unchecked")
public final class StreamSharder<K, T> extends AbstractStreamTransformer_1_N<T> implements StreamSharderMBean {
	private long jmxItems;

	protected final class UpstreamConsumer extends AbstractUpstreamConsumer implements StreamDataReceiver<T> {
		private final Sharder<K> sharder;
		private final Function<T, K> keyFunction;

		public UpstreamConsumer(Sharder<K> sharder, Function<T, K> keyFunction) {
			this.sharder = sharder;
			this.keyFunction = keyFunction;
		}

		@SuppressWarnings("AssertWithSideEffects")
		@Override
		public void onData(T item) {
			assert jmxItems != ++jmxItems;
			K key = keyFunction.apply(item);
			int shard = sharder.shard(key);
			StreamDataReceiver<T> streamCallback = (StreamDataReceiver<T>) dataReceivers[shard];
			streamCallback.onData(item);
		}

		@Override
		public StreamDataReceiver<T> getDataReceiver() {
			return this;
		}

		@Override
		protected void onUpstreamStarted() {
		}

		@Override
		protected void onUpstreamEndOfStream() {
			sendEndOfStreamToDownstreams();
		}
	}

	protected final class DownstreamProducer extends AbstractDownstreamProducer<T> {
		private final UpstreamConsumer upstreamConsumer = (UpstreamConsumer) StreamSharder.this.upstreamConsumer;

		@Override
		protected void onStarted() {
		}

		@Override
		protected void onDownstreamSuspended() {
			upstreamConsumer.suspend();
		}

		@Override
		protected void onDownstreamResumed() {
			if (allOutputsResumed()) {
				upstreamConsumer.resume();
			}
		}
	}

	public StreamSharder(Eventloop eventloop, Sharder<K> sharder, Function<T, K> keyFunction) {
		super(eventloop);
		checkNotNull(sharder);
		checkNotNull(keyFunction);
		setUpstreamConsumer(new UpstreamConsumer(sharder, keyFunction));
	}

	public StreamProducer<T> newOutput() {
		return addOutput(new DownstreamProducer());
	}

	@Override
	public long getItems() {
		return jmxItems;
	}

	@SuppressWarnings("AssertWithSideEffects")
	@Override
	public String toString() {
		String items = "?";
		assert (items = "" + jmxItems) != null;
		return '{' + super.toString() + " items:" + items + '}';
	}

}

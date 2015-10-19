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

import com.google.gson.Gson;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.AbstractStreamTransformer_1_1;
import io.datakernel.stream.StreamDataReceiver;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.Math.max;
import static java.lang.Math.min;

/**
 * Represent a serializer which receives streams of objects of specify type and converts it to
 * json and streams it  forth
 *
 * @param <T> type of received objects
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public final class StreamGsonSerializer<T> extends AbstractStreamTransformer_1_1<T, ByteBuf> implements StreamSerializer<T>, StreamGsonSerializerMBean {
	private static final ArrayIndexOutOfBoundsException OUT_OF_BOUNDS_EXCEPTION = new ArrayIndexOutOfBoundsException();

	private final UpstreamConsumer upstreamConsumer;
	private final DownstreamProducer downstreamProducer;

	private final class UpstreamConsumer extends AbstractUpstreamConsumer {

		@Override
		protected void onUpstreamEndOfStream() {
			downstreamProducer.flushBuffer(downstreamProducer.getDownstreamDataReceiver());
			downstreamProducer.sendEndOfStream();
		}

		@Override
		public StreamDataReceiver<T> getDataReceiver() {
			return downstreamProducer;
		}
	}

	private final class DownstreamProducer extends AbstractDownstreamProducer implements StreamDataReceiver<T> {
		private final BufferAppendable appendable = new BufferAppendable();
		private final int defaultBufferSize;
		private final int maxMessageSize;

		private final Gson gson;
		private final Class<T> type;

		private int estimatedMessageSize;

		private final int flushDelayMillis;
		private boolean flushPosted;

		private ByteBuf buf;

		private int jmxItems;
		private int jmxBufs;
		private long jmxBytes;

		public DownstreamProducer(Gson gson, Class<T> type, int defaultBufferSize, int maxMessageSize, int flushDelayMillis) {
			checkArgument(maxMessageSize > 0, "maxMessageSize must be positive value, got %s", maxMessageSize);
			checkArgument(defaultBufferSize > 0, "defaultBufferSize must be positive value, got %s", defaultBufferSize);

			this.maxMessageSize = maxMessageSize;
			this.gson = checkNotNull(gson);
			this.type = checkNotNull(type);
			this.defaultBufferSize = defaultBufferSize;
			this.estimatedMessageSize = 1;
			this.flushDelayMillis = flushDelayMillis;
			allocateBuffer();
		}

		@Override
		protected void onDownstreamSuspended() {
			upstreamConsumer.suspend();
		}

		@Override
		protected void onDownstreamResumed() {
			upstreamConsumer.resume();
		}

		/**
		 * After receiving data it serialize it to json and adds it to the appendable,
		 * and flushes bytes depending on the autoFlushDelay
		 *
		 * @param value received value
		 */
		@Override
		public void onData(T value) {
			//noinspection AssertWithSideEffects
			assert jmxItems != ++jmxItems;
			for (; ; ) {
				ensureSize(estimatedMessageSize);
				int positionBegin = appendable.position();
				try {
					gson.toJson(value, type, appendable);
					appendable.append((char) 0);
					int positionEnd = appendable.position();
					int size = positionEnd - positionBegin;
					assert size != 0;
					if (size > maxMessageSize) {
						closeWithError(OUT_OF_BOUNDS_EXCEPTION);
						return;
					}
					size += size >>> 2;
					if (size > estimatedMessageSize)
						estimatedMessageSize = size;
					else
						estimatedMessageSize -= estimatedMessageSize >>> 8;
					break;
				} catch (BufferAppendableException e) {
					appendable.position(positionBegin);
					int size = appendable.array().length - positionBegin;
					if (size >= maxMessageSize) {
						closeWithError(e);
						return;
					}
					estimatedMessageSize = size + 1 + (size >>> 1);
				} catch (Exception e) {
					closeWithError(e);
					return;
				}
			}
			if (!flushPosted) {
				postFlush();
			}
		}

		private void allocateBuffer() {
			buf = ByteBufPool.allocate(min(maxMessageSize, max(defaultBufferSize, estimatedMessageSize)));
			appendable.set(buf.array(), 0);
		}

		private void ensureSize(int size) {
			if (appendable.remaining() < size) {
				flushBuffer(downstreamDataReceiver);
			}
		}

		private void postFlush() {
			flushPosted = true;
			if (flushDelayMillis == 0) {
				eventloop.postLater(new Runnable() {
					@Override
					public void run() {
						if (isStatusReady()) {
							flush();
						}
					}
				});
			} else {
				eventloop.schedule(eventloop.currentTimeMillis() + (long) flushDelayMillis, new Runnable() {
					@Override
					public void run() {
						if (isStatusReady()) {
							flush();
						}
					}
				});
			}
		}

		private void flushBuffer(StreamDataReceiver<ByteBuf> receiver) {
			buf.position(0);
			int size = appendable.position();
			if (size != 0) {
				buf.limit(size);
				jmxBytes += size;
				jmxBufs++;
				receiver.onData(buf);
			} else {
				buf.recycle();
			}
			allocateBuffer();
		}

		/**
		 * Bytes will be sending immediately.
		 */
		private void flush() {
			flushBuffer(downstreamDataReceiver);
			flushPosted = false;
		}

		@Override
		protected void doCleanup() {
			if (buf != null) {
				buf.recycle();
				buf = null;
			}
			for (ByteBuf buf : downstreamProducer.bufferedList) {
				buf.recycle();
			}
			downstreamProducer.bufferedList.clear();
		}

	}

	/**
	 * Creates a new instance of this class
	 *
	 * @param eventloop         event loop in which serializer will run
	 * @param gson              gson for converting
	 * @param type              type of received object
	 * @param defaultBufferSize default size for ByteBuffer
	 * @param maxMessageSize    maximal size of message which this serializer can receive
	 */
	public StreamGsonSerializer(Eventloop eventloop, Gson gson, Class<T> type, int defaultBufferSize, int maxMessageSize, int flushDelayMillis) {
		super(eventloop);
		this.upstreamConsumer = new UpstreamConsumer();
		this.downstreamProducer = new DownstreamProducer(gson, type, defaultBufferSize, maxMessageSize, flushDelayMillis);
	}

	/**
	 * Bytes will be sending immediately.
	 */
	@Override
	public void flush() {
		downstreamProducer.flush();
	}

	@Override
	public int getItems() {
		return downstreamProducer.jmxItems;
	}

	@Override
	public int getBufs() {
		return downstreamProducer.jmxBufs;
	}

	@Override
	public long getBytes() {
		return downstreamProducer.jmxBytes;
	}

	@SuppressWarnings({"AssertWithSideEffects", "ConstantConditions"})
	@Override
	public String toString() {
		boolean assertOn = false;
		assert assertOn = true;
		return '{' + super.toString()
				+ " items:" + (assertOn ? "" + downstreamProducer.jmxItems : "?")
				+ " bufs:" + downstreamProducer.jmxBufs
				+ " bytes:" + downstreamProducer.jmxBytes + '}';
	}

}
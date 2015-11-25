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

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.stream.AbstractStreamTransformer_1_1;
import io.datakernel.stream.StreamDataReceiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.Math.max;

/**
 * Represent serializer which serializes data from some type to ByteBuffer.It is a {@link AbstractStreamTransformer_1_1}
 * which receives specified type and streams ByteBufs . It is one of implementation of {@link StreamSerializer}.
 *
 * @param <T> original type of data
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public final class StreamBinarySerializer<T> extends AbstractStreamTransformer_1_1<T, ByteBuf> implements StreamSerializer<T>, StreamBinarySerializerMBean {
	private static final Logger logger = LoggerFactory.getLogger(StreamBinarySerializer.class);
	private static final ArrayIndexOutOfBoundsException OUT_OF_BOUNDS_EXCEPTION = new ArrayIndexOutOfBoundsException();

	public static final int MAX_SIZE_1_BYTE = 127; // (1 << (1 * 7)) - 1
	public static final int MAX_SIZE_2_BYTE = 16383; // (1 << (2 * 7)) - 1
	public static final int MAX_SIZE_3_BYTE = 2097151; // (1 << (3 * 7)) - 1
	public static final int MAX_SIZE = MAX_SIZE_3_BYTE;

	private static final int MAX_HEADER_BYTES = 3;

	private final InputConsumer inputConsumer;
	private final OutputProducer outputProducer;

	private final class InputConsumer extends AbstractInputConsumer {

		@Override
		protected void onUpstreamEndOfStream() {
			outputProducer.flushAndClose();
		}

		@Override
		public StreamDataReceiver<T> getDataReceiver() {
			return outputProducer;
		}
	}

	private final class OutputProducer extends AbstractOutputProducer implements StreamDataReceiver<T> {
		private final BufferSerializer<T> serializer;

		private final int defaultBufferSize;
		private final int maxMessageSize;
		private final int headerSize;

		// TODO (dvolvach): queue of serialized buffers
		private ByteBuf byteBuf;
		private int outputBufferPos;
		private int estimatedMessageSize;

		private final int flushDelayMillis;
		private boolean flushPosted;

		private final boolean skipSerializationErrors;
		private int serializationErrors;

		private int jmxItems;
		private long jmxBytes;
		private int jmxBufs;

		public OutputProducer(BufferSerializer<T> serializer, int defaultBufferSize, int maxMessageSize, int flushDelayMillis, boolean skipSerializationErrors) {
			checkArgument(maxMessageSize > 0 && maxMessageSize <= MAX_SIZE, "maxMessageSize must be in [4B..2MB) range, got %s", maxMessageSize);
			checkArgument(defaultBufferSize > 0, "defaultBufferSize must be positive value, got %s", defaultBufferSize);

			this.skipSerializationErrors = skipSerializationErrors;
			this.serializer = checkNotNull(serializer);
			this.maxMessageSize = maxMessageSize;
			this.headerSize = varint32Size(maxMessageSize);
			this.estimatedMessageSize = 1;
			this.defaultBufferSize = defaultBufferSize;
			this.flushDelayMillis = flushDelayMillis;
			allocateBuffer();
		}

		@Override
		protected void onDownstreamSuspended() {
			inputConsumer.suspend();
		}

		@Override
		protected void onDownstreamResumed() {
			inputConsumer.resume();
			resumeProduce();
		}

		private int varint32Size(int value) {
			if ((value & 0xffffffff << 7) == 0) return 1;
			if ((value & 0xffffffff << 14) == 0) return 2;
			if ((value & 0xffffffff << 21) == 0) return 3;
			if ((value & 0xffffffff << 28) == 0) return 4;
			return 5;
		}

		private void allocateBuffer() {
			byteBuf = ByteBufPool.allocate(max(defaultBufferSize, headerSize + estimatedMessageSize));
			outputBufferPos = 0;
		}

		private void flushBuffer(StreamDataReceiver<ByteBuf> receiver) {
			byteBuf.position(0);
			int size = outputBufferPos;
			if (size != 0) {
				byteBuf.limit(size);
				jmxBytes += size;
				jmxBufs++;
				if (outputProducer.getProducerStatus().isOpen()) {
					receiver.onData(byteBuf);
				}
			} else {
				byteBuf.recycle();
			}
			allocateBuffer();
		}

		private void ensureSize(int size) {
			if (byteBuf.array().length - outputBufferPos < size) {
				flushBuffer(outputProducer.getDownstreamDataReceiver());
			}
		}

		private void writeSize(byte[] buf, int pos, int size) {
			if (headerSize == 1) {
				buf[pos] = (byte) size;
				return;
			}

			buf[pos] = (byte) ((size & 0x7F) | 0x80);
			size >>>= 7;
			if (headerSize == 2) {
				buf[pos + 1] = (byte) size;
				return;
			}

			assert headerSize == 3;

			buf[pos + 1] = (byte) ((size & 0x7F) | 0x80);
			size >>>= 7;
			buf[pos + 2] = (byte) size;
		}

		/**
		 * After receiving data it serializes it to buffer and adds it to the outputBuffer,
		 * and flushes bytes depending on the autoFlushDelay
		 *
		 * @param value receiving item
		 */
		@Override
		public void onData(T value) {
			//noinspection AssertWithSideEffects
			assert jmxItems != ++jmxItems;
			for (; ; ) {
				ensureSize(headerSize + estimatedMessageSize);
				int positionBegin = outputBufferPos;
				int positionItem = positionBegin + headerSize;
				try {
					outputBufferPos = positionItem;
					outputBufferPos = serializer.serialize(byteBuf.array(), outputBufferPos, value);
					int positionEnd = outputBufferPos;
					int messageSize = positionEnd - positionItem;
					assert messageSize != 0;
					if (messageSize > maxMessageSize) {
						handleSerializationError(OUT_OF_BOUNDS_EXCEPTION);
						return;
					}
					writeSize(byteBuf.array(), positionBegin, messageSize);
					messageSize += messageSize >>> 2;
					if (messageSize > estimatedMessageSize)
						estimatedMessageSize = messageSize;
					else
						estimatedMessageSize -= estimatedMessageSize >>> 8;
					break;
				} catch (ArrayIndexOutOfBoundsException e) {
					outputBufferPos = positionBegin;
					int messageSize = byteBuf.array().length - positionItem;
					if (messageSize >= maxMessageSize) {
						handleSerializationError(e);
						return;
					}
					estimatedMessageSize = messageSize + 1 + (messageSize >>> 1);
				} catch (Exception e) {
					handleSerializationError(e);
					return;
				}
			}
			if (!flushPosted) {
				postFlush();
			}
		}

		private void flushAndClose() {
			flushBuffer(outputProducer.getDownstreamDataReceiver());
			byteBuf.recycle();
			byteBuf = null;
			logger.trace("endOfStream {}, upstream: {}", this, inputConsumer.getUpstream());
			outputProducer.sendEndOfStream();
		}

		private void handleSerializationError(Exception e) {
			serializationErrors++;
			if (skipSerializationErrors) {
				logger.warn("Skipping serialization error in {} : {}", this, e.toString());
			} else {
				closeWithError(e);
			}
		}

		/**
		 * Bytes will be sent immediately.
		 */
		private void flush() {
			flushBuffer(outputProducer.getDownstreamDataReceiver());
			flushPosted = false;
		}

		private void postFlush() {
			flushPosted = true;
			if (flushDelayMillis == 0) {
				eventloop.postLater(new Runnable() {
					@Override
					public void run() {
						if (outputProducer.getProducerStatus().isOpen()) {
							flush();
						}
					}
				});
			} else {
				eventloop.scheduleBackground(eventloop.currentTimeMillis() + flushDelayMillis, new Runnable() {
					@Override
					public void run() {
						if (outputProducer.getProducerStatus().isOpen()) {
							flush();
						}
					}
				});
			}
		}

		@Override
		protected void doCleanup() {
			if (byteBuf != null) {
				byteBuf.recycle();
				byteBuf = null;
			}
		}
	}

	/**
	 * Creates a new instance of this class
	 *
	 * @param eventloop      event loop in which serializer will run
	 * @param serializer     specified BufferSerializer for this type
	 * @param maxMessageSize maximal size of message which this serializer can receive
	 */
	public StreamBinarySerializer(Eventloop eventloop, BufferSerializer<T> serializer, int defaultBufferSize, int maxMessageSize, int flushDelayMillis, boolean skipSerializationErrors) {
		super(eventloop);
		this.inputConsumer = new InputConsumer();
		this.outputProducer = new OutputProducer(serializer, defaultBufferSize, maxMessageSize, flushDelayMillis, skipSerializationErrors);
	}

	/**
	 * Bytes will be sent immediately.
	 */
	@Override
	public void flush() {
		outputProducer.flush();
	}

	// JMX

	@Override
	public int getItems() {
		return outputProducer.jmxItems;
	}

	@Override
	public int getBufs() {
		return outputProducer.jmxBufs;
	}

	@Override
	public long getBytes() {
		return outputProducer.jmxBytes;
	}

	@SuppressWarnings("AssertWithSideEffects")
	@Override
	public int getSerializationErrors() {
		return outputProducer.serializationErrors;
	}

	@SuppressWarnings({"AssertWithSideEffects", "ConstantConditions"})
	@Override
	public String toString() {
		boolean assertOn = false;
		assert assertOn = true;
		return '{' + super.toString()
				+ (outputProducer.serializationErrors != 0 ? "serializationErrors:" + outputProducer.serializationErrors : "")
				+ " items:" + (assertOn ? "" + outputProducer.jmxItems : "?")
				+ " bufs:" + outputProducer.jmxBufs
				+ " bytes:" + outputProducer.jmxBytes + '}';
	}
}
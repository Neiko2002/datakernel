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

import io.datakernel.bytebufnew.ByteBufN;
import io.datakernel.bytebufnew.ByteBufNPool;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.AbstractStreamTransformer_1_1;
import io.datakernel.stream.StreamDataReceiver;

public final class StreamByteChunker extends AbstractStreamTransformer_1_1<ByteBufN, ByteBufN> {
	private final InputConsumer inputConsumer;
	private final OutputProducer outputProducer;

	protected final class InputConsumer extends AbstractInputConsumer {

		@Override
		protected void onUpstreamEndOfStream() {
			outputProducer.flushAndClose();
		}

		@Override
		public StreamDataReceiver<ByteBufN> getDataReceiver() {
			return outputProducer;
		}
	}

	protected final class OutputProducer extends AbstractOutputProducer implements StreamDataReceiver<ByteBufN> {
		private final int minChunkSize;
		private final int maxChunkSize;
		private ByteBufN internalBuf;

		public OutputProducer(int minChunkSize, int maxChunkSize) {
			this.minChunkSize = minChunkSize;
			this.maxChunkSize = maxChunkSize;
			this.internalBuf = ByteBufNPool.allocateAtLeast(maxChunkSize);
		}

		@Override
		protected void onDownstreamSuspended() {
			inputConsumer.suspend();
		}

		@Override
		protected void onDownstreamResumed() {
			inputConsumer.resume();
		}

		@Override
		public void onData(ByteBufN buf) {
			while (internalBuf.getWritePosition() + buf.remainingToRead() >= minChunkSize) {
				if (internalBuf.getWritePosition() == 0) {
					int chunkSize = Math.min(maxChunkSize, buf.remainingToRead());
					send(buf.slice(chunkSize));
					buf.setReadPosition(buf.getReadPosition() + chunkSize);
				} else {
					buf.drainTo(internalBuf, minChunkSize - internalBuf.getWritePosition());
					send(internalBuf);
					internalBuf = ByteBufNPool.allocateAtLeast(maxChunkSize);
				}
			}

			buf.drainTo(internalBuf, buf.remainingToRead());
			assert internalBuf.getWritePosition() < minChunkSize;

			buf.recycle();
		}

		private void flushAndClose() {
			if (internalBuf.canRead()) {
				outputProducer.send(internalBuf);
			} else {
				internalBuf.recycle();
			}
			internalBuf = null;
			outputProducer.sendEndOfStream();
		}

		@Override
		protected void doCleanup() {
			if (internalBuf != null) {
				internalBuf.recycle();
				internalBuf = null;
			}
		}
	}

	public StreamByteChunker(Eventloop eventloop, int minChunkSize, int maxChunkSize) {
		super(eventloop);
		this.inputConsumer = new InputConsumer();
		this.outputProducer = new OutputProducer(minChunkSize, maxChunkSize);
	}

}

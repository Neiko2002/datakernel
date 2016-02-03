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

package io.datakernel.stream.file;

import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.file.AsyncFile;
import io.datakernel.stream.AbstractStreamProducer;
import io.datakernel.stream.StreamStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.datakernel.async.AsyncCallbacks.ignoreCompletionCallback;
import static java.lang.Math.min;
import static java.nio.file.StandardOpenOption.READ;

/**
 * This class allows you to read data from file non-blocking. It represents a {@link AbstractStreamProducer}
 * which streams data from file.
 */
public final class StreamFileReader extends AbstractStreamProducer<ByteBuf> {
	private static final Logger logger = LoggerFactory.getLogger(StreamFileReader.class);

	private final ExecutorService executor;
	protected AsyncFile asyncFile; // TODO (arashev): consider making it final, to decouple file opening logic from the stream itself
	private Path path;

	protected final int bufferSize;
	protected long position;
	protected long length;

	protected boolean pendingAsyncOperation;

	private ResultCallback<Long> positionCallback;

	public StreamFileReader(Eventloop eventloop, ExecutorService executor,
	                        int bufferSize,
	                        Path path, long position, long length) {
		super(eventloop);
		this.executor = checkNotNull(executor);
		this.bufferSize = bufferSize;
		this.path = path;
		this.position = position;
		this.length = length;
	}

	/**
	 * Returns new StreamFileReader for reading file segment
	 *
	 * @param eventloop  event loop in which it will work
	 * @param executor   executor in which file will be opened
	 * @param bufferSize size of buffer, size of data which can be read at once
	 * @param path       location of file
	 * @param position   position after which reader will read file
	 * @param length     number of elements for reading
	 */
	public static StreamFileReader readFileSegment(Eventloop eventloop, ExecutorService executor,
	                                               int bufferSize,
	                                               Path path, long position, long length) {
		return new StreamFileReader(eventloop, executor, bufferSize, path, position, length);
	}

	/**
	 * Returns new StreamFileReader for full reading file
	 *
	 * @param eventloop  event loop in which it will work
	 * @param executor   executor it which file will be opened
	 * @param bufferSize size of buffer, size of data which can be read at once
	 * @param path       location of file
	 */
	public static StreamFileReader readFileFully(Eventloop eventloop, ExecutorService executor,
	                                             int bufferSize,
	                                             Path path) {
		return new StreamFileReader(eventloop, executor, bufferSize, path, 0, Long.MAX_VALUE);
	}

	/**
	 * Returns new StreamFileReader for reading file after some position
	 *
	 * @param eventloop  event loop in which it will work
	 * @param executor   executor it which file will be opened
	 * @param bufferSize size of buffer, size of data which can be read at once
	 * @param path       location of file
	 * @param position   position after which reader will read file
	 */
	public static StreamFileReader readFileFrom(Eventloop eventloop, ExecutorService executor,
	                                            int bufferSize,
	                                            Path path, long position) {
		return new StreamFileReader(eventloop, executor, bufferSize, path, position, Long.MAX_VALUE);
	}

	public void setPositionCallback(ResultCallback<Long> positionCallback) {
		if (getProducerStatus().isOpen()) {
			this.positionCallback = positionCallback;
		} else {
			if (getProducerStatus() == StreamStatus.END_OF_STREAM) {
				positionCallback.onResult(position);
			} else {
				positionCallback.onException(getProducerException());
			}
		}
	}

	protected void doFlush() {
		if (getProducerStatus().isClosed() || asyncFile == null)
			return;

		if (length == 0L) {
			doCleanup();
			sendEndOfStream();
			return;
		}

		final ByteBuf buf = ByteBufPool.allocate((int) min(bufferSize, length));

		asyncFile.read(buf, position, new ResultCallback<Integer>() {
			@Override
			public void onResult(Integer result) {
				if (getProducerStatus().isClosed()) {
					buf.recycle();
					doCleanup();
					return;
				}
				pendingAsyncOperation = false;
				if (result == -1) {
					buf.recycle();
					doCleanup();
					sendEndOfStream();

					if (positionCallback != null)
						positionCallback.onResult(position);

					return;
				} else {
					position += result;
					buf.flip();
					send(buf);
					if (length != Long.MAX_VALUE)
						length -= result;
				}
				if (isStatusReady())
					postFlush();
			}

			@Override
			public void onException(Exception e) {
				buf.recycle();
				doCleanup();
				closeWithError(e);

				if (positionCallback != null)
					positionCallback.onException(e);
			}
		});
	}

	protected void postFlush() {
		if (asyncFile == null || pendingAsyncOperation)
			return;
		pendingAsyncOperation = true;
		eventloop.post(new Runnable() {
			@Override
			public void run() {
				doFlush();
			}
		});
	}

	@Override
	public void onSuspended() {
		logger.trace("{}: downstream consumer {} suspended.", this, downstreamConsumer);
	}

	@Override
	public void onResumed() {
		postFlush();
	}

	@Override
	protected void onStarted() {
		if (asyncFile != null || pendingAsyncOperation)
			return;
		pendingAsyncOperation = true;
		logger.info("Opening file at path {}", path);
		AsyncFile.open(eventloop, executor, path, new OpenOption[]{READ}, new ResultCallback<AsyncFile>() {
			@Override
			public void onResult(AsyncFile file) {
				pendingAsyncOperation = false;
				asyncFile = file;
				postFlush();
			}

			@Override
			public void onException(Exception exception) {
				closeWithError(exception);

				if (positionCallback != null)
					positionCallback.onException(exception);
			}
		});
	}

	@Override
	protected void onDataReceiverChanged() {

	}

	@Override
	protected void onError(Exception e) {
		logger.error("{}: onError", this, e);
	}

	@Override
	protected void doCleanup() {
		if (asyncFile != null) {
			logger.info("Closing file at path {}", path);
			asyncFile.close(ignoreCompletionCallback());
			asyncFile = null;
		}
	}

	public long getPosition() {
		return position;
	}

	@Override
	public String toString() {
		return "StreamFileReader{" + path +
				", pos=" + position +
				(length != Long.MAX_VALUE ? ", len=" + length : "") +
				(pendingAsyncOperation ? ", pendingAsyncOperation" : "") +
				'}';
	}
}


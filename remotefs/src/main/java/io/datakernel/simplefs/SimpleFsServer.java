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

package io.datakernel.simplefs;

import io.datakernel.FileSystem;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ForwardingCompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.protocol.FsServer;
import io.datakernel.protocol.ServerProtocol;
import io.datakernel.stream.StreamProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static io.datakernel.async.AsyncCallbacks.ignoreCompletionCallback;
import static io.datakernel.codegen.utils.Preconditions.check;
import static io.datakernel.simplefs.SimpleFsServer.ServerStatus.RUNNING;
import static io.datakernel.simplefs.SimpleFsServer.ServerStatus.SHUTDOWN;
import static io.datakernel.util.Preconditions.checkNotNull;

public final class SimpleFsServer extends FsServer implements EventloopService {
	public static final class Builder {
		private final Eventloop eventloop;
		private final ServerProtocol.Builder<ServerProtocol.Builder, SimpleFsServer> protocolBuilder;
		private final List<InetSocketAddress> addresses = new ArrayList<>();

		private long approveWaitTime = DEFAULT_APPROVE_WAIT_TIME;

		private ExecutorService executor;
		private Path storage;
		private Path tmpStorage;

		private int readerBufferSize = FileSystem.DEFAULT_READER_BUFFER_SIZE;
		private String inProgressExtension = FileSystem.DEFAULT_IN_PROGRESS_EXTENSION;

		public Builder(Eventloop eventloop, ExecutorService executor, Path storage, Path tmpStorage) {
			this.eventloop = eventloop;
			this.protocolBuilder = ServerProtocol.build(eventloop);
			this.executor = executor;
			this.storage = storage;
			this.tmpStorage = tmpStorage;
		}

		public Builder setApproveWaitTime(long approveWaitTime) {
			this.approveWaitTime = approveWaitTime;
			return this;
		}

		public Builder setListenAddress(InetSocketAddress address) {
			this.addresses.add(address);
			return this;
		}

		public Builder setListenAddresses(List<InetSocketAddress> addresses) {
			this.addresses.addAll(addresses);
			return this;
		}

		public Builder setListenPort(int port) {
			this.addresses.add(new InetSocketAddress(port));
			return this;
		}

		// filesystem
		public Builder setInProgressExtension(String inProgressExtension) {
			this.inProgressExtension = inProgressExtension;
			return this;
		}

		public Builder setReaderBufferSize(int readerBufferSize) {
			this.readerBufferSize = readerBufferSize;
			return this;
		}

		// protocol
		public Builder setDeserializerBufferSize(int deserializerBufferSize) {
			protocolBuilder.setDeserializerBufferSize(deserializerBufferSize);
			return this;
		}

		public Builder setSerializerBufferSize(int serializerBufferSize) {
			protocolBuilder.setSerializerBufferSize(serializerBufferSize);
			return this;
		}

		public Builder setSerializerFlushDelayMillis(int serializerFlushDelayMillis) {
			protocolBuilder.setSerializerFlushDelayMillis(serializerFlushDelayMillis);
			return this;
		}

		public Builder setSerializerMaxMessageSize(int serializerMaxMessageSize) {
			protocolBuilder.setSerializerMaxMessageSize(serializerMaxMessageSize);
			return this;
		}

		public SimpleFsServer build() {
			FileSystem fs = FileSystem.newInstance(eventloop, executor, storage, tmpStorage,
					readerBufferSize, inProgressExtension);
			ServerProtocol<SimpleFsServer> protocol = protocolBuilder.build();
			protocol.setListenAddresses(addresses);
			SimpleFsServer server = new SimpleFsServer(eventloop, fs, protocol, approveWaitTime);
			protocol.wire(server);
			return server;
		}
	}

	private static final Logger logger = LoggerFactory.getLogger(SimpleFsServer.class);

	public static final long DEFAULT_APPROVE_WAIT_TIME = 10 * 100;

	private final Eventloop eventloop;
	private final FileSystem fileSystem;
	private final ServerProtocol protocol;

	private final Set<String> filesToBeCommitted = new HashSet<>();
	private final long approveWaitTime;

	private CompletionCallback callbackOnStop;
	private ServerStatus serverStatus;

	// creators
	private SimpleFsServer(Eventloop eventLoop, FileSystem fileSystem, ServerProtocol protocol, long approveWaitTime) {
		this.eventloop = checkNotNull(eventLoop);
		this.fileSystem = checkNotNull(fileSystem);
		this.protocol = checkNotNull(protocol);
		check(approveWaitTime > 0, "Approve wait time should be positive: %s", approveWaitTime);
		this.approveWaitTime = approveWaitTime;
	}

	public static SimpleFsServer newInstance(Eventloop eventloop, ExecutorService executor,
	                                         Path storage, Path tmpStorage, int port) {
		return new Builder(eventloop, executor, storage, tmpStorage)
				.setListenPort(port)
				.build();
	}

	public static Builder build(Eventloop eventloop, ExecutorService executor, Path storage, Path tmpStorage) {
		return new Builder(eventloop, executor, storage, tmpStorage);
	}

	// start/stop service methods
	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@Override
	public void start(final CompletionCallback callback) {
		logger.info("Starting SimpleFS");
		if (serverStatus == RUNNING) {
			callback.onComplete();
		} else {
			try {
				fileSystem.initDirectories();
				protocol.listen();
				serverStatus = RUNNING;
				callback.onComplete();
			} catch (IOException e) {
				callback.onException(e);
			}
		}
	}

	@Override
	public void stop(final CompletionCallback callback) {
		logger.info("Stopping SimpleFS");
		serverStatus = SHUTDOWN;
		if (filesToBeCommitted.isEmpty()) {
			protocol.close();
			callback.onComplete();
		} else {
			callbackOnStop = callback;
		}
	}

	// functional
	@Override
	protected void upload(final String fileName, StreamProducer<ByteBuf> producer, final CompletionCallback callback) {
		logger.info("Received command to upload file: {}", fileName);
		check(serverStatus == RUNNING, "Server shut down!");
		fileSystem.saveToTmp(fileName, producer, new ForwardingCompletionCallback(callback) {
			@Override
			public void onComplete() {
				logger.trace("File {} flushed to tmp", fileName);
				filesToBeCommitted.add(fileName);
				scheduleTmpFileDeletion(fileName);
				callback.onComplete();
			}
		});
	}

	@Override
	protected void commit(final String fileName, final boolean success, final CompletionCallback callback) {
		logger.info("Received command to commit file: {}, {}", fileName, success);
		check(serverStatus == RUNNING || filesToBeCommitted.contains(fileName), "Server shut down!");
		filesToBeCommitted.remove(fileName);

		CompletionCallback cb = new CompletionCallback() {
			@Override
			public void onComplete() {
				logger.info("Committed file: {}, status: {}", fileName, success);
				callback.onComplete();
				onOperationFinished();
			}

			@Override
			public void onException(Exception e) {
				logger.error("Can't commit file: {}", fileName, e);
				callback.onException(e);
				onOperationFinished();
			}
		};

		if (success) {
			fileSystem.commitTmp(fileName, cb);
		} else {
			fileSystem.deleteTmp(fileName, cb);
		}
	}

	@Override
	protected StreamProducer<ByteBuf> download(String fileName, long startPosition) {
		logger.info("Received command to download file: {}, start position: {}", fileName, startPosition);
		check(serverStatus == RUNNING, "Server shut down!");
		return fileSystem.get(fileName, startPosition);
	}

	@Override
	protected void delete(String fileName, CompletionCallback callback) {
		logger.info("Received command to delete file: {}", fileName);
		check(serverStatus == RUNNING, "Server shut down!");
		fileSystem.delete(fileName, callback);
	}

	@Override
	protected void list(ResultCallback<Set<String>> callback) {
		logger.info("Received command to list files");
		check(serverStatus == RUNNING, "Server shut down!");
		fileSystem.list(callback);
	}

	@Override
	protected long fileSize(String fileName) {
		return fileSystem.fileSize(fileName);
	}

	private void scheduleTmpFileDeletion(final String fileName) {
		eventloop.scheduleBackground(eventloop.currentTimeMillis() + approveWaitTime, new Runnable() {
			@Override
			public void run() {
				if (filesToBeCommitted.contains(fileName)) {
					commit(fileName, false, ignoreCompletionCallback());
				}
			}
		});
	}

	private void onOperationFinished() {
		if (serverStatus == SHUTDOWN && filesToBeCommitted.isEmpty()) {
			protocol.close();
			callbackOnStop.onComplete();
		}
	}

	enum ServerStatus {
		RUNNING, SHUTDOWN
	}
}
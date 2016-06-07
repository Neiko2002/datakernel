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

package io.datakernel;

import com.google.gson.Gson;
import io.datakernel.FsCommands.*;
import io.datakernel.FsResponses.*;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.async.SimpleCompletionCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.AbstractServer;
import io.datakernel.eventloop.AsyncTcpSocket;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.net.Messaging.MessageOrEndOfStream;
import io.datakernel.stream.net.MessagingConnection;
import io.datakernel.stream.net.MessagingSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.datakernel.stream.net.MessagingSerializers.ofGson;

public abstract class FsServer<S extends FsServer<S>> extends AbstractServer<S> {
	protected final Logger logger = LoggerFactory.getLogger(this.getClass());
	protected final FileManager fileManager;
	private MessagingSerializer<FsCommand, FsResponse> serializer = ofGson(getCommandGSON(), FsCommand.class, getResponseGson(), FsResponse.class);

	protected final Map<Class, MessagingHandler> handlers;

	// creators & builder methods
	protected FsServer(Eventloop eventloop, FileManager fileManager) {
		super(eventloop);
		this.fileManager = fileManager;
		this.handlers = createHandlers();
	}

	// abstract core methods
	protected abstract void upload(String filePath, ResultCallback<StreamConsumer<ByteBuf>> callback);

	protected abstract void download(String filePath, long startPosition, ResultCallback<StreamProducer<ByteBuf>> callback);

	protected abstract void delete(String filePath, CompletionCallback callback);

	protected abstract void list(ResultCallback<List<String>> callback);

	// set up connection
	@Override
	protected final AsyncTcpSocket.EventHandler createSocketHandler(AsyncTcpSocket asyncTcpSocket) {
		final MessagingConnection<FsCommand, FsResponse> messaging = new MessagingConnection<>(eventloop, asyncTcpSocket, serializer);
		messaging.read(new ResultCallback<MessageOrEndOfStream<FsCommand>>() {
			@Override
			public void onResult(MessageOrEndOfStream<FsCommand> result) {
				if (result.isEndOfStream()) {
					logger.warn("unexpected end of stream");
				} else {
					onRead(messaging, result.getMessage());
				}
			}

			@Override
			public void onException(Exception e) {
				logger.error("received error while reading", e);
				messaging.close();
			}
		});
		return messaging;
	}

	private void onRead(MessagingConnection<FsCommand, FsResponse> messaging, FsCommand item) {
		MessagingHandler handler = handlers.get(item.getClass());
		if (handler == null) {
			messaging.close();
			logger.error("missing handler for " + item);
		} else {
			//noinspection unchecked
			handler.onMessage(messaging, item);
		}
	}

	protected Gson getResponseGson() {
		return FsResponses.responseGson;
	}

	protected Gson getCommandGSON() {
		return FsCommands.commandGSON;
	}

	protected interface MessagingHandler<I, O> {
		void onMessage(MessagingConnection<I, O> messaging, I item);
	}

	private Map<Class, MessagingHandler> createHandlers() {
		Map<Class, MessagingHandler> map = new HashMap<>();
		map.put(Upload.class, new UploadMessagingHandler());
		map.put(Download.class, new DownloadMessagingHandler());
		map.put(Delete.class, new DeleteMessagingHandler());
		map.put(ListFiles.class, new ListFilesMessagingHandler());
		return map;
	}

	// handler classes
	private class UploadMessagingHandler implements MessagingHandler<Upload, FsResponse> {
		@Override
		public void onMessage(final MessagingConnection<Upload, FsResponse> messaging, final Upload item) {
			messaging.write(new Ok(), new CompletionCallback() {
				@Override
				public void onComplete() {
					upload(item.filePath, new ResultCallback<StreamConsumer<ByteBuf>>() {
						@Override
						public void onResult(StreamConsumer<ByteBuf> result) {
							messaging.readStream(result, new CompletionCallback() {
								@Override
								public void onComplete() {
									messaging.write(new Acknowledge(), new SimpleCompletionCallback() {
										@Override
										protected void onCompleteOrException() {
											messaging.close();
										}
									});
								}

								@Override
								public void onException(Exception e) {
									messaging.write(new Err(e.getMessage()), new SimpleCompletionCallback() {
										@Override
										protected void onCompleteOrException() {
											messaging.close();
										}
									});
								}
							});
						}

						@Override
						public void onException(Exception e) {
							messaging.write(new Err(e.getMessage()), new SimpleCompletionCallback() {
								@Override
								protected void onCompleteOrException() {
									messaging.close();
								}
							});
						}
					});
				}

				@Override
				public void onException(Exception e) {
					messaging.write(new Err(e.getMessage()), new SimpleCompletionCallback() {
						@Override
						protected void onCompleteOrException() {
							messaging.close();
						}
					});
				}
			});
		}
	}

	private class DownloadMessagingHandler implements MessagingHandler<Download, FsResponse> {
		@Override
		public void onMessage(final MessagingConnection<Download, FsResponse> messaging, final Download item) {
			fileManager.size(item.filePath, new ResultCallback<Long>() {
				@Override
				public void onResult(final Long size) {
					if (size < 0) {
						messaging.write(new Err("File not found"), new SimpleCompletionCallback() {
							@Override
							protected void onCompleteOrException() {
								messaging.close();
							}
						});
					} else {
						download(item.filePath, item.startPosition, new ResultCallback<StreamProducer<ByteBuf>>() {
							@Override
							public void onResult(final StreamProducer<ByteBuf> result) {
								messaging.write(new Ready(size), new CompletionCallback() {
									@Override
									public void onComplete() {
										messaging.writeStream(result, new SimpleCompletionCallback() {
											@Override
											protected void onCompleteOrException() {
												messaging.close();
											}
										});
									}

									@Override
									public void onException(Exception e) {
										messaging.write(new Err(e.getMessage()), new SimpleCompletionCallback() {
											@Override
											protected void onCompleteOrException() {
												messaging.close();
											}
										});
									}
								});
							}

							@Override
							public void onException(Exception e) {
								messaging.write(new Err(e.getMessage()), new SimpleCompletionCallback() {
									@Override
									protected void onCompleteOrException() {
										messaging.close();
									}
								});
							}
						});
					}
				}

				@Override
				public void onException(Exception e) {
					messaging.write(new Err(e.getMessage()), new SimpleCompletionCallback() {
						@Override
						protected void onCompleteOrException() {
							messaging.close();
						}
					});
				}
			});
		}
	}

	private class DeleteMessagingHandler implements MessagingHandler<Delete, FsResponse> {
		@Override
		public void onMessage(final MessagingConnection<Delete, FsResponse> messaging, final Delete item) {
			delete(item.filePath, new CompletionCallback() {
				@Override
				public void onComplete() {
					messaging.write(new Ok(), new SimpleCompletionCallback() {
						@Override
						protected void onCompleteOrException() {
							messaging.close();
						}
					});
				}

				@Override
				public void onException(Exception e) {
					messaging.write(new Err(e.getMessage()), new SimpleCompletionCallback() {
						@Override
						protected void onCompleteOrException() {
							messaging.close();
						}
					});
				}
			});
		}
	}

	private class ListFilesMessagingHandler implements MessagingHandler<ListFiles, FsResponse> {
		@Override
		public void onMessage(final MessagingConnection<ListFiles, FsResponse> messaging, ListFiles item) {
			list(new ResultCallback<List<String>>() {
				@Override
				public void onResult(List<String> result) {
					messaging.write(new ListOfFiles(result), new SimpleCompletionCallback() {
						@Override
						protected void onCompleteOrException() {
							messaging.close();
						}
					});
				}

				@Override
				public void onException(Exception e) {
					messaging.write(new Err(e.getMessage()), new SimpleCompletionCallback() {
						@Override
						protected void onCompleteOrException() {
							messaging.close();
						}
					});
				}
			});
		}
	}
}
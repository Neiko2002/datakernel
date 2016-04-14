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

package io.datakernel.stream.net;

import com.google.common.collect.Lists;
import com.google.common.net.InetAddresses;
import com.google.gson.Gson;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.eventloop.*;
import io.datakernel.stream.*;
import io.datakernel.stream.processor.StreamBinaryDeserializer;
import io.datakernel.stream.processor.StreamBinarySerializer;
import io.datakernel.stream.processor.StreamGsonDeserializer;
import io.datakernel.stream.processor.StreamGsonSerializer;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;

import static io.datakernel.bytebuf.ByteBufPool.getPoolItemsString;
import static io.datakernel.eventloop.SocketReconnector.reconnect;
import static io.datakernel.net.SocketSettings.defaultSocketSettings;
import static io.datakernel.serializer.asm.BufferSerializers.intSerializer;
import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;

public final class TcpStreamSocketConnectionTest {
	private static final int LISTEN_PORT = 1234;
	private static final InetSocketAddress address = new InetSocketAddress(InetAddresses.forString("127.0.0.1"), LISTEN_PORT);

	@Before
	public void setUp() throws Exception {
		ByteBufPool.clear();
		ByteBufPool.setSizes(0, Integer.MAX_VALUE);
	}

	@Test
	public void test() throws Exception {
		final List<Integer> source = Lists.newArrayList();
		for (int i = 0; i < 5; i++) {
			source.add(i);
		}

		final Eventloop eventloop = new Eventloop();

		final StreamConsumers.ToList<Integer> consumerToList = StreamConsumers.toList(eventloop);

		AbstractServer server = new AbstractServer(eventloop) {
			@Override
			protected SocketConnection createConnection(SocketChannel socketChannel) {
				return new TcpStreamSocketConnection(eventloop, socketChannel) {
					@Override
					protected void wire(StreamProducer<ByteBuf> socketReader, StreamConsumer<ByteBuf> socketWriter) {
						StreamBinaryDeserializer<Integer> streamDeserializer = new StreamBinaryDeserializer<>(eventloop, intSerializer(), 10);
						streamDeserializer.getOutput().streamTo(consumerToList);
						socketReader.streamTo(streamDeserializer.getInput());
					}
				};
			}
		};
		server.setListenAddress(address).acceptOnce();
		server.listen();

		final StreamBinarySerializer<Integer> streamSerializer = new StreamBinarySerializer<>(eventloop, intSerializer(), 1, 10, 0, false);
		reconnect(eventloop, address, defaultSocketSettings(), 3, 100L, new ConnectCallback() {
			@Override
			protected void onConnect(SocketChannel socketChannel) {
				SocketConnection connection = new TcpStreamSocketConnection(eventloop, socketChannel) {
					@Override
					protected void wire(StreamProducer<ByteBuf> socketReader, StreamConsumer<ByteBuf> socketWriter) {
						streamSerializer.getOutput().streamTo(socketWriter);
						StreamProducers.ofIterable(eventloop, source).streamTo(streamSerializer.getInput());
					}
				};
				connection.register();
			}

			@Override
			protected void onException(Exception exception) {
				fail();
			}
		});

		eventloop.run();

		assertEquals(source, consumerToList.getList());

		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testLoopback() throws Exception {
		final List<Integer> source = Lists.newArrayList();
		for (int i = 0; i < 100; i++) {
			source.add(i);
		}

		final Eventloop eventloop = new Eventloop();

		final StreamConsumers.ToList<Integer> consumerToList = StreamConsumers.toList(eventloop);

		AbstractServer server = new AbstractServer(eventloop) {
			@Override
			protected SocketConnection createConnection(SocketChannel socketChannel) {
				return new TcpStreamSocketConnection(eventloop, socketChannel) {
					@Override
					protected void wire(StreamProducer<ByteBuf> socketReader, StreamConsumer<ByteBuf> socketWriter) {
						socketReader.streamTo(socketWriter);
					}
				};
			}
		};
		server.setListenAddress(address).acceptOnce();
		server.listen();

		final StreamBinarySerializer<Integer> streamSerializer = new StreamBinarySerializer<>(eventloop, intSerializer(), 1, 10, 0, false);
		final StreamBinaryDeserializer<Integer> streamDeserializer = new StreamBinaryDeserializer<>(eventloop, intSerializer(), 10);
		reconnect(eventloop, address, defaultSocketSettings(), 3, 100L, new ConnectCallback() {
			@Override
			protected void onConnect(SocketChannel socketChannel) {
				SocketConnection connection = new TcpStreamSocketConnection(eventloop, socketChannel) {
					@Override
					protected void wire(StreamProducer<ByteBuf> socketReader, StreamConsumer<ByteBuf> socketWriter) {
						streamSerializer.getOutput().streamTo(socketWriter);
						socketReader.streamTo(streamDeserializer.getInput());
					}
				};
				connection.register();
				StreamProducers.ofIterable(eventloop, source).streamTo(streamSerializer.getInput());
				streamDeserializer.getOutput().streamTo(consumerToList);
			}

			@Override
			protected void onException(Exception exception) {
				fail();
			}
		});

		eventloop.run();

		assertEquals(source, consumerToList.getList());

		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testLoopbackWithError() throws Exception {
		final List<Integer> source = Lists.newArrayList();
		for (int i = 0; i < 100; i++) {
			source.add(i);
		}

		final Eventloop eventloop = new Eventloop();

		List<Integer> list = new ArrayList<>();
		final TestStreamConsumers.TestConsumerToList<Integer> consumerToListWithError = new TestStreamConsumers.TestConsumerToList<Integer>(eventloop, list) {
			@Override
			public void onData(Integer item) {
				super.onData(item);
				if (list.size() == 50) {
					closeWithError(new Exception("Test Exception"));
					return;
				}
			}
		};

		AbstractServer server = new AbstractServer(eventloop) {
			@Override
			protected SocketConnection createConnection(SocketChannel socketChannel) {
				return new TcpStreamSocketConnection(eventloop, socketChannel) {
					@Override
					protected void wire(StreamProducer<ByteBuf> socketReader, StreamConsumer<ByteBuf> socketWriter) {
						socketReader.streamTo(socketWriter);
					}
				};
			}
		};
		server.setListenAddress(address).acceptOnce();
		server.listen();

		final StreamGsonSerializer<Integer> streamSerializer = new StreamGsonSerializer<>(eventloop, new Gson(), Integer.class, 1, 50, 0);
		final StreamGsonDeserializer<Integer> streamDeserializer = new StreamGsonDeserializer<>(eventloop, new Gson(), Integer.class, 10);
		reconnect(eventloop, address, defaultSocketSettings(), 3, 100L, new ConnectCallback() {
			@Override
			protected void onConnect(SocketChannel socketChannel) {
				SocketConnection connection = new TcpStreamSocketConnection(eventloop, socketChannel) {
					@Override
					protected void wire(StreamProducer<ByteBuf> socketReader, StreamConsumer<ByteBuf> socketWriter) {
						streamSerializer.getOutput().streamTo(socketWriter);
						socketReader.streamTo(streamDeserializer.getInput());
					}
				};
				connection.register();
				StreamProducers.ofIterable(eventloop, source).streamTo(streamSerializer.getInput());
				streamDeserializer.getOutput().streamTo(consumerToListWithError);
			}

			@Override
			protected void onException(Exception exception) {
				fail();
			}
		});

		eventloop.run();

		assertEquals(list.size(), 50);

		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testGsonWithError() throws Exception {
		final List<Integer> source = Lists.newArrayList();
		for (int i = 0; i < 100; i++) {
			source.add(i);
		}

		final Eventloop eventloop = new Eventloop();

		List<Integer> list = new ArrayList<>();
		final TestStreamConsumers.TestConsumerToList<Integer> consumerToListWithError = new TestStreamConsumers.TestConsumerToList<Integer>(eventloop, list) {
			@Override
			public void onData(Integer item) {
				super.onData(item);
				if (list.size() == 50) {
					closeWithError(new Exception("Test Exception"));
					return;
				}
			}
		};

		AbstractServer server = new AbstractServer(eventloop) {
			@Override
			protected SocketConnection createConnection(SocketChannel socketChannel) {
				return new TcpStreamSocketConnection(eventloop, socketChannel) {
					@Override
					protected void wire(StreamProducer<ByteBuf> socketReader, StreamConsumer<ByteBuf> socketWriter) {
						final StreamGsonDeserializer<Integer> streamDeserializer = new StreamGsonDeserializer<>(eventloop, new Gson(), Integer.class, 10);
						streamDeserializer.getOutput().streamTo(consumerToListWithError);
						socketReader.streamTo(streamDeserializer.getInput());
					}
				};
			}
		};
		server.setListenAddress(address).acceptOnce();
		server.listen();

		final StreamGsonSerializer<Integer> streamSerializer = new StreamGsonSerializer<>(eventloop, new Gson(), Integer.class, 1, 50, 0);
		reconnect(eventloop, address, defaultSocketSettings(), 3, 100L, new ConnectCallback() {
			@Override
			protected void onConnect(SocketChannel socketChannel) {
				SocketConnection connection = new TcpStreamSocketConnection(eventloop, socketChannel) {
					@Override
					protected void wire(StreamProducer<ByteBuf> socketReader, StreamConsumer<ByteBuf> socketWriter) {
						streamSerializer.getOutput().streamTo(socketWriter);
						StreamProducers.ofIterable(eventloop, source).streamTo(streamSerializer.getInput());
					}
				};
				connection.register();
			}

			@Override
			protected void onException(Exception exception) {
				fail();
			}
		});

		eventloop.run();

		assertEquals(50, list.size());

		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}
}

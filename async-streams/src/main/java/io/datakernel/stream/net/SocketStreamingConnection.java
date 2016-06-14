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

import io.datakernel.async.CompletionCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.AsyncTcpSocket;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represent the TCP connection which  processes received items with {@link StreamProducer} and {@link StreamConsumer},
 * which organized by binary protocol. It is created with socketChannel and sides exchange ByteBufs.
 */
public final class SocketStreamingConnection implements AsyncTcpSocket.EventHandler, SocketStreaming {
	private static final Logger logger = LoggerFactory.getLogger(SocketStreamingConnection.class);

	private final Eventloop eventloop;
	private final AsyncTcpSocket asyncTcpSocket;

	private final SocketStreamProducer socketReader;
	private final SocketStreamConsumer socketWriter;

	/**
	 * Returns new instance of TcpStreamSocketConnection
	 *
	 * @param eventloop eventloop in with this connection will be handled
	 */
	public SocketStreamingConnection(Eventloop eventloop, final AsyncTcpSocket asyncTcpSocket) {
		this.eventloop = eventloop;
		this.asyncTcpSocket = asyncTcpSocket;
		this.socketWriter = new SocketStreamConsumer(eventloop, asyncTcpSocket, new CompletionCallback() {
			@Override
			public void onException(Exception exception) {
				socketReader.closeWithError(exception);
				asyncTcpSocket.close();
			}

			@Override
			public void onComplete() {
			}
		});
		this.socketReader = new SocketStreamProducer(eventloop, asyncTcpSocket, new CompletionCallback() {
			@Override
			public void onException(Exception exception) {
				socketWriter.closeWithError(exception);
				asyncTcpSocket.close();
			}

			@Override
			public void onComplete() {
			}
		});
		socketReader.streamTo(StreamConsumers.<ByteBuf>idle(eventloop));
		new StreamProducers.EndOfStream<ByteBuf>(eventloop).streamTo(socketWriter);
	}

	@Override
	public void sendStreamFrom(StreamProducer<ByteBuf> producer, CompletionCallback callback) {
		producer.streamTo(socketWriter);
	}

	@Override
	public void receiveStreamTo(StreamConsumer<ByteBuf> consumer, CompletionCallback callback) {
		socketReader.streamTo(consumer);
	}

	/**
	 * Is called after connection registration. Wires socketReader with StreamConsumer specified by,
	 * and socketWriter with StreamProducer, that are specified by overridden method {@code wire} of subclass.
	 * If StreamConsumer is null, items from socketReader are ignored. If StreamProducer is null, socketWriter
	 * gets EndOfStream signal.
	 */
	@Override
	public void onRegistered() {
	}

	/**
	 * Sends received bytes to StreamConsumer
	 */
	@Override
	public void onRead(ByteBuf buf) {
		assert eventloop.inEventloopThread();
		socketReader.onRead(buf);
	}

	@Override
	public void onReadEndOfStream() {
		socketReader.onReadEndOfStream();
	}

	@Override
	public void onWrite() {
		socketWriter.onWrite();
	}

	@Override
	public void onClosedWithError(Exception e) {
		socketReader.closeWithError(e);
		socketWriter.closeWithError(e);
	}

	@Override
	public String toString() {
		return "{asyncTcpSocket=" + asyncTcpSocket + '}';
	}
}

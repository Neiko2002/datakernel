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

package io.datakernel.eventloop;

import io.datakernel.bytebufnew.ByteBufN;
import io.datakernel.bytebufnew.ByteBufNPool;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.DatagramChannel;

import static io.datakernel.bytebufnew.ByteBufNPool.getPoolItemsString;
import static io.datakernel.eventloop.Eventloop.createDatagramChannel;
import static io.datakernel.net.DatagramSocketSettings.defaultDatagramSocketSettings;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class UdpSocketHandlerTest {
	private static final int SERVER_PORT = 45555;
	private static final InetSocketAddress SERVER_ADDRESS = new InetSocketAddress("127.0.0.1", SERVER_PORT);
	private Eventloop eventloop = new Eventloop();

	private final byte[] bytesToSend = new byte[]{-127, 100, 0, 5, 11, 13, 17, 99};

	private AsyncUdpSocketImpl getEchoServerUdpSocket(DatagramChannel serverChannel) {
		final AsyncUdpSocketImpl socket = new AsyncUdpSocketImpl(eventloop, serverChannel);
		socket.setEventHandler(new AsyncUdpSocket.EventHandler() {
			@Override
			public void onRegistered() {
				socket.read();
			}

			@Override
			public void onRead(UdpPacket packet) {
				socket.send(packet);
			}

			@Override
			public void onSent() {
				socket.close();
			}

			@Override
			public void onClosedWithError(Exception e) {
				// empty
			}
		});
		return socket;
	}

	private AsyncUdpSocketImpl getClientUdpSocket(DatagramChannel clientChannel) {
		final AsyncUdpSocketImpl socket = new AsyncUdpSocketImpl(eventloop, clientChannel);
		socket.setEventHandler(new AsyncUdpSocket.EventHandler() {
			@Override
			public void onRegistered() {
				sendTestData(bytesToSend, SERVER_ADDRESS);
			}

			@Override
			public void onRead(UdpPacket packet) {
				byte[] bytesReceived = packet.getBuf().array();
				byte[] message = new byte[packet.getBuf().remainingToRead()];

				System.arraycopy(bytesReceived, 0, message, 0, packet.getBuf().remainingToRead());
				assertArrayEquals(bytesToSend, message);

				packet.recycle();
				socket.close();
			}

			@Override
			public void onSent() {
				socket.read();
			}

			@Override
			public void onClosedWithError(Exception e) {
				throw new AssertionError(e);
			}

			void sendTestData(byte[] data, InetSocketAddress address) {
				socket.send(new UdpPacket(ByteBufN.wrap(data), address));
			}
		});
		return socket;
	}

	@Before
	public void setUp() throws Exception {
		ByteBufNPool.clear();
		ByteBufNPool.setSizes(0, Integer.MAX_VALUE);
	}

	@Test
	public void testEchoUdpServer() throws Exception {
		eventloop.post(new Runnable() {
			@Override
			public void run() {
				try {
					//  server
					DatagramChannel serverChannel = createDatagramChannel(defaultDatagramSocketSettings(), SERVER_ADDRESS, null);
					AsyncUdpSocketImpl serverConnection = getEchoServerUdpSocket(serverChannel);
					serverConnection.register();

					// client
					DatagramChannel clientChannel = createDatagramChannel(defaultDatagramSocketSettings(), null, null);
					AsyncUdpSocketImpl clientConnection = getClientUdpSocket(clientChannel);
					clientConnection.register();

				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		});

		eventloop.run();

		assertEquals(getPoolItemsString(), ByteBufNPool.getCreatedItems(), ByteBufNPool.getPoolItems());
	}
}
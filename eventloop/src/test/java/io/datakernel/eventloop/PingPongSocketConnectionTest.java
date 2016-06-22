package io.datakernel.eventloop;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.net.SocketSettings;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;

import static io.datakernel.bytebuf.ByteBufPool.*;
import static io.datakernel.util.ByteBufStrings.decodeAscii;
import static io.datakernel.util.ByteBufStrings.wrapAscii;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class PingPongSocketConnectionTest {
	private final InetSocketAddress ADDRESS = new InetSocketAddress("localhost", 9022);
	private final int ITERATIONS = 3;
	private final String RESPONSE_MSG = "PONG";
	private final String REQUEST_MSG = "PING";

	@Test
	public void test() throws IOException {
		final Eventloop eventloop = new Eventloop();

		final AbstractServer ppServer = new AbstractServer(eventloop) {
			@Override
			protected AsyncTcpSocket.EventHandler createSocketHandler(final AsyncTcpSocket asyncTcpSocket) {
				return new ServerConnection(asyncTcpSocket);
			}
		};
		ppServer.setListenAddress(ADDRESS);
		ppServer.listen();

		eventloop.connect(ADDRESS, new SocketSettings(), new ConnectCallback() {
			@Override
			public AsyncTcpSocket.EventHandler onConnect(AsyncTcpSocketImpl clientTcpSocket) {
				return new ClientConnection(clientTcpSocket, ppServer);
			}

			@Override
			public void onException(Exception e) {
				fail("Exception: " + e);
			}
		});

		eventloop.run();
	}

	private class ServerConnection implements AsyncTcpSocket.EventHandler {
		private final AsyncTcpSocket asyncTcpSocket;
		int counter;

		public ServerConnection(AsyncTcpSocket asyncTcpSocket) {
			this.asyncTcpSocket = asyncTcpSocket;
			counter = 0;
		}

		@Override
		public void onRegistered() {
			asyncTcpSocket.read();
		}

		@Override
		public void onReadEndOfStream() {
			asyncTcpSocket.close();
			assertEquals(ITERATIONS, counter);
		}

		@Override
		public void onRead(ByteBuf buf) {
			assertEquals(REQUEST_MSG, decodeAscii(buf));
			buf.recycle();
			counter++;
			asyncTcpSocket.write(wrapAscii(RESPONSE_MSG));

		}

		@Override
		public void onWrite() {
			asyncTcpSocket.read();
		}

		@Override
		public void onClosedWithError(Exception e) {
			e.printStackTrace();
		}
	}

	private class ClientConnection implements AsyncTcpSocket.EventHandler {
		private final AsyncTcpSocketImpl clientTcpSocket;
		private final AbstractServer server;
		int counter;

		public ClientConnection(AsyncTcpSocketImpl clientTcpSocket, AbstractServer server) {
			this.clientTcpSocket = clientTcpSocket;
			this.server = server;
			counter = 0;
		}

		@Override
		public void onRegistered() {
			clientTcpSocket.write(wrapAscii(REQUEST_MSG));
		}

		@Override
		public void onRead(ByteBuf buf) {
			assertEquals(RESPONSE_MSG, decodeAscii(buf));
			if (++counter == ITERATIONS) {
				clientTcpSocket.close();
				server.close();
			} else {
				clientTcpSocket.write(wrapAscii(REQUEST_MSG));
			}
			buf.recycle();
		}

		@Override
		public void onReadEndOfStream() {
		}

		@Override
		public void onWrite() {
			clientTcpSocket.read();
		}

		@Override
		public void onClosedWithError(Exception e) {
			e.printStackTrace();
		}
	}
}

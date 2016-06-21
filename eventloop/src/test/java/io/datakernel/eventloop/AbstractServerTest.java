package io.datakernel.eventloop;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.AsyncTcpSocket.EventHandler;
import io.datakernel.net.SocketSettings;
import io.datakernel.util.ByteBufStrings;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;

import static io.datakernel.bytebuf.ByteBufPool.*;
import static io.datakernel.net.SocketSettings.defaultSocketSettings;
import static org.junit.Assert.assertEquals;

public class AbstractServerTest {
	@Test
	public void testTimeouts() throws IOException {
		Eventloop eventloop = new Eventloop();

		InetSocketAddress address = new InetSocketAddress(5588);
		final SocketSettings settings = SocketSettings.defaultSocketSettings().readTimeout(100L).writeTimeout(100L);

		final AbstractServer server = new AbstractServer(eventloop) {
			@Override
			protected EventHandler createSocketHandler(final AsyncTcpSocket asyncTcpSocket) {
				return new EventHandler() {
					@Override
					public void onRegistered() {
						asyncTcpSocket.read();
					}

					@Override
					public void onRead(final ByteBuf buf) {
						eventloop.schedule(eventloop.currentTimeMillis() + 5, new Runnable() {
							@Override
							public void run() {
								asyncTcpSocket.write(buf);
							}
						});
					}

					@Override
					public void onReadEndOfStream() {
						asyncTcpSocket.close();
					}

					@Override
					public void onWrite() {
						asyncTcpSocket.close();
					}

					@Override
					public void onClosedWithError(Exception e) {
						asyncTcpSocket.close();
					}
				};
			}
		};
		server.socketSettings(settings);
		server.setListenAddress(address);

		server.listen();

		eventloop.connect(address, defaultSocketSettings(), 100, new ConnectCallback() {
			@Override
			public EventHandler onConnect(final AsyncTcpSocketImpl asyncTcpSocket) {
				settings.applyReadWriteTimeoutsTo(asyncTcpSocket);
				return new EventHandler() {
					@Override
					public void onRegistered() {
						asyncTcpSocket.write(ByteBufStrings.wrapAscii("Hello!"));
						asyncTcpSocket.read();
					}

					@Override
					public void onRead(ByteBuf buf) {
						buf.recycle();
						asyncTcpSocket.close();
						server.close();
					}

					@Override
					public void onReadEndOfStream() {
						asyncTcpSocket.close();
						server.close();
					}

					@Override
					public void onWrite() {
					}

					@Override
					public void onClosedWithError(Exception e) {
						asyncTcpSocket.close();
					}
				};
			}

			@Override
			public void onException(Exception e) {
				e.printStackTrace();
			}
		});

		eventloop.run();
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}
}
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

package io.datakernel.http;

import io.datakernel.async.AsyncCancellable;
import io.datakernel.eventloop.*;
import io.datakernel.jmx.ValueStats;
import io.datakernel.util.Stopwatch;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static io.datakernel.http.AbstractHttpConnection.MAX_HEADER_LINE_SIZE;

/**
 * A HttpServer is bound to an IP address and port number and listens for incoming connections
 * from clients on this address. A HttpServer is supported  {@link AsyncHttpServlet} that completes all responses asynchronously.
 */
public final class AsyncHttpServer extends AbstractServer<AsyncHttpServer> {
	private static final long CHECK_PERIOD = 1000L;
	private static final long MAX_IDLE_CONNECTION_TIME = 30 * 1000L;

	private final ExposedLinkedList<AbstractHttpConnection> connectionsList;
	private final Runnable expiredConnectionsTask = createExpiredConnectionsTask();

	private final AsyncHttpServlet servlet;

	private AsyncCancellable scheduleExpiredConnectionCheck;
	private final char[] headerChars;
	private int maxHttpMessageSize = Integer.MAX_VALUE;

	// SSL
	private SSLContext sslContext;
	private ExecutorService executor;

	//JMX
	private final ValueStats timeCheckExpired;
	private final ValueStats expiredConnections;
	private boolean monitoring;
	private volatile double smoothingWindow = 10.0;

	/**
	 * Creates new instance of AsyncHttpServer
	 *
	 * @param eventloop eventloop in which will handle this connection
	 * @param servlet   servlet for handling requests
	 */
	public AsyncHttpServer(Eventloop eventloop, AsyncHttpServlet servlet) {
		super(eventloop);
		this.connectionsList = new ExposedLinkedList<>();
		this.servlet = servlet;
		char[] chars = eventloop.get(char[].class);
		if (chars == null || chars.length < MAX_HEADER_LINE_SIZE) {
			chars = new char[MAX_HEADER_LINE_SIZE];
			eventloop.set(char[].class, chars);
		}
		this.headerChars = chars;

		// JMX
		this.timeCheckExpired = new ValueStats();
		this.expiredConnections = new ValueStats();
	}

	public AsyncHttpServer enableSsl(SSLContext sslContext, ExecutorService executor) {
		this.sslContext = sslContext;
		this.executor = executor;
		return this;
	}

	public AsyncHttpServer setMaxHttpMessageSize(int size) {
		this.maxHttpMessageSize = size;
		return this;
	}

	private Runnable createExpiredConnectionsTask() {
		return new Runnable() {
			@Override
			public void run() {
				checkExpiredConnections();
				if (!connectionsList.isEmpty())
					scheduleExpiredConnectionCheck();
			}
		};
	}

	private void scheduleExpiredConnectionCheck() {
		scheduleExpiredConnectionCheck = eventloop.schedule(eventloop.currentTimeMillis() + CHECK_PERIOD, expiredConnectionsTask);
	}

	private int checkExpiredConnections() {
		scheduleExpiredConnectionCheck = null;
		Stopwatch stopwatch = (monitoring) ? Stopwatch.createStarted() : null;
		int count = 0;
		try {
			final long now = eventloop.currentTimeMillis();

			ExposedLinkedList.Node<AbstractHttpConnection> node = connectionsList.getFirstNode();
			while (node != null) {
				AbstractHttpConnection connection = node.getValue();
				node = node.getNext();

				assert eventloop.inEventloopThread();
				long idleTime = now - connection.getActivityTime();
				if (idleTime > MAX_IDLE_CONNECTION_TIME) {
					connection.close(); // self removing from this pool
					count++;
				}
			}
			expiredConnections.recordValue(count);
		} finally {
			if (stopwatch != null)
				timeCheckExpired.recordValue((int) stopwatch.elapsed(TimeUnit.MICROSECONDS));
		}
		return count;
	}

	@Override
	protected AsyncTcpSocket.EventHandler createSocketHandler(AsyncTcpSocketImpl asyncTcpSocket) {
		assert eventloop.inEventloopThread();

		AsyncSslSocket asyncSslSocket = null;
		if (sslContext != null) {
			SSLEngine ssl = sslContext.createSSLEngine();
			ssl.setUseClientMode(false);
			asyncSslSocket = new AsyncSslSocket(eventloop, asyncTcpSocket, ssl, executor);
		}

		HttpServerConnection connection = new HttpServerConnection(eventloop, asyncTcpSocket.getRemoteSocketAddress().getAddress(),
				asyncSslSocket != null ? asyncSslSocket : asyncTcpSocket,
				servlet, connectionsList, headerChars, maxHttpMessageSize);

		if (asyncSslSocket != null) {
			asyncSslSocket.setEventHandler(connection);
		}

		if (connectionsList.isEmpty())
			scheduleExpiredConnectionCheck();
		return asyncSslSocket != null ? asyncSslSocket : connection;
	}

	/**
	 * Closes all connections from this server
	 */
	@Override
	protected void onClose() {
		closeConnections();
	}

	private void closeConnections() {
		if (scheduleExpiredConnectionCheck != null)
			scheduleExpiredConnectionCheck.cancel();

		ExposedLinkedList.Node<AbstractHttpConnection> node = connectionsList.getFirstNode();
		while (node != null) {
			AbstractHttpConnection connection = node.getValue();
			node = node.getNext();

			assert eventloop.inEventloopThread();
			connection.close();
		}
	}

	@Override
	protected void onListen() {
	}

	// JMX

//	@JmxAttribute
//	public JmxRefreshableStats<?> getConnectionsCount() {
//		return JmxStatsWrappers.forSummableValue(connectionsList.size());
//	}

	// TODO (vmykhalko)
/*	@Override
	public void startMonitoring() {
		monitoring = true;
	}

	@Override
	public void stopMonitoring() {
		monitoring = false;
	}

	@Override
	public void resetStats() {
		timeCheckExpired.resetStats();
	}

	@Override
	public int getTimeCheckExpiredMicros() {
		return (int)timeCheckExpired.getLastValue();
	}

	@Override
	public String getTimeCheckExpiredMicrosStats() {
		return timeCheckExpired.toString();
	}

	@Override
	public String getExpiredConnectionsStats() {
		return expiredConnections.toString();
	}

	@Override
	public int getConnectionsCount() {
		return connectionsList.size();
	}

	@Override
	public String[] getConnections() {
		Joiner joiner = Joiner.on(',');
		List<String> info = new ArrayList<>();
		info.add("RemoteSocketAddress,isRegistered,LifeTime,ActivityTime");
		for (Node<AbstractHttpConnection> node = connectionsList.getFirstNode(); node != null; node = node.getNext()) {
			AbstractHttpConnection connection = node.getValue();
			String string = joiner.join(connection.getRemoteSocketAddress(), connection.isRegistered(),
					MBeanFormat.formatPeriodAgo(connection.getLifeTime()),
					MBeanFormat.formatPeriodAgo(connection.getActivityTime())
			);
			info.add(string);
		}
		return info.toArray(new String[info.size()]);
	}
	*/
}

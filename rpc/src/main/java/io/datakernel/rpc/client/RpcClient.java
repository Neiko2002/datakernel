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

package io.datakernel.rpc.client;

import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.async.ResultCallbackFuture;
import io.datakernel.eventloop.ConnectCallback;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.eventloop.NioService;
import io.datakernel.jmx.LastExceptionCounter;
import io.datakernel.net.SocketSettings;
import io.datakernel.rpc.client.RpcClientConnection.StatusListener;
import io.datakernel.rpc.client.jmx.RpcJmxClient;
import io.datakernel.rpc.client.jmx.RpcJmxClientConnection;
import io.datakernel.rpc.client.jmx.RpcJmxConnectsStatsSet;
import io.datakernel.rpc.client.jmx.RpcJmxRequestsStatsSet;
import io.datakernel.rpc.client.sender.RpcNoSenderException;
import io.datakernel.rpc.client.sender.RpcSender;
import io.datakernel.rpc.client.sender.RpcStrategy;
import io.datakernel.rpc.protocol.*;
import io.datakernel.util.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static io.datakernel.async.AsyncCallbacks.postCompletion;
import static io.datakernel.async.AsyncCallbacks.postException;
import static io.datakernel.rpc.protocol.stream.RpcStreamProtocolFactory.streamProtocol;
import static io.datakernel.util.Preconditions.checkNotNull;
import static io.datakernel.util.Preconditions.checkState;

public final class RpcClient implements NioService, RpcJmxClient {
	public static final SocketSettings DEFAULT_SOCKET_SETTINGS = new SocketSettings().tcpNoDelay(true);
	public static final int DEFAULT_CONNECT_TIMEOUT = 10 * 1000;
	public static final int DEFAULT_RECONNECT_INTERVAL = 1 * 1000;

	private Logger logger = LoggerFactory.getLogger(RpcClient.class);

	private final NioEventloop eventloop;
	private RpcStrategy strategy;
	private List<InetSocketAddress> addresses;
	private final Map<InetSocketAddress, RpcClientConnection> connections = new HashMap<>();
	private RpcProtocolFactory protocolFactory = streamProtocol();
	private final RpcSerializer serializer;
	private SocketSettings socketSettings = DEFAULT_SOCKET_SETTINGS;
	private int connectTimeoutMillis = DEFAULT_CONNECT_TIMEOUT;
	private int reconnectIntervalMillis = DEFAULT_RECONNECT_INTERVAL;

	private RpcSender requestSender;

	private CompletionCallback startCallback;
	private boolean running;

	// JMX
	private static final double DEFAULT_SMOOTING_WINDOW = 10.0;    // 10 seconds
	private static final double DEFAULT_SMOOTHING_PRECISION = 0.1; // 0.1 second

	private boolean monitoring;

	private double smoothingWindow = DEFAULT_SMOOTING_WINDOW;
	private double smoothingPrecision = DEFAULT_SMOOTHING_PRECISION;

	private final RpcJmxRequestsStatsSet generalRequestsStats;
	private final RpcJmxConnectsStatsSet generalConnectsStats;
	private final Map<Class<?>, RpcJmxRequestsStatsSet> requestStatsPerClass;
	private final Map<InetSocketAddress, RpcJmxConnectsStatsSet> connectsStatsPerAddress;

	private final RpcClientConnectionPool pool = new RpcClientConnectionPool() {
		@Override
		public RpcClientConnection get(InetSocketAddress address) {
			return connections.get(address);
		}
	};

	private RpcClient(NioEventloop eventloop, RpcSerializer serializer) {
		this.eventloop = eventloop;
		this.serializer = serializer;

		// JMX
		this.generalRequestsStats = new RpcJmxRequestsStatsSet(smoothingWindow, smoothingPrecision, eventloop);
		this.generalConnectsStats = new RpcJmxConnectsStatsSet(smoothingWindow, smoothingPrecision, eventloop);
		this.requestStatsPerClass = new HashMap<>();
		this.connectsStatsPerAddress = new HashMap<>(); // TODO(vmykhalko): properly initialize this map with addresses, and add new addresses when needed
	}

	public static RpcClient create(final NioEventloop eventloop, final RpcSerializer serializerFactory) {
		return new RpcClient(eventloop, serializerFactory);
	}

	public RpcClient strategy(RpcStrategy requestSendingStrategy) {
		this.strategy = requestSendingStrategy;
		this.addresses = new ArrayList<>(requestSendingStrategy.getAddresses());

		// jmx
		for (InetSocketAddress address : this.addresses) {
			if (!connectsStatsPerAddress.containsKey(address)) {
				connectsStatsPerAddress.put(address,
						new RpcJmxConnectsStatsSet(smoothingWindow, smoothingPrecision, eventloop));
			}
		}

		return this;
	}

	public RpcClient protocol(RpcProtocolFactory protocolFactory) {
		this.protocolFactory = protocolFactory;
		return this;
	}

	public RpcClient socketSettings(SocketSettings socketSettings) {
		this.socketSettings = checkNotNull(socketSettings);
		return this;
	}

	public SocketSettings getSocketSettings() {
		return socketSettings;
	}

	public RpcClient logger(Logger logger) {
		this.logger = checkNotNull(logger);
		return this;
	}

	public RpcClient connectTimeoutMillis(int connectTimeoutMillis) {
		this.connectTimeoutMillis = connectTimeoutMillis;
		return this;
	}

	public RpcClient reconnectIntervalMillis(int reconnectIntervalMillis) {
		this.reconnectIntervalMillis = reconnectIntervalMillis;
		return this;
	}

	@Override
	public NioEventloop getNioEventloop() {
		return eventloop;
	}

	@Override
	public void start(CompletionCallback callback) {
		checkState(eventloop.inEventloopThread());
		checkNotNull(callback);
		checkState(!running);
		running = true;
		startCallback = callback;
		if (connectTimeoutMillis != 0) {
			eventloop.scheduleBackground(eventloop.currentTimeMillis() + connectTimeoutMillis, new Runnable() {
				@Override
				public void run() {
					if (running && startCallback != null) {
						String errorMsg = String.format("Some of the required servers did not respond within %.1f sec",
								connectTimeoutMillis / 1000.0);
						postException(eventloop, startCallback, new InterruptedException(errorMsg));
						running = false;
						startCallback = null;
					}
				}
			});
		}

		for (InetSocketAddress address : addresses) {
			connect(address);
		}
	}

	public void stop() {
		checkState(eventloop.inEventloopThread());
		checkState(running);
		running = false;
		if (startCallback != null) {
			postException(eventloop, startCallback, new InterruptedException("Start aborted"));
			startCallback = null;
		}
		closeConnections();
	}

	@Override
	public void stop(final CompletionCallback callback) {
		checkNotNull(callback);
		stop();
		callback.onComplete();
	}

	private void connect(final InetSocketAddress address) {
		if (!running) {
			return;
		}

		logger.info("Connecting {}", address);
		eventloop.connect(address, socketSettings, new ConnectCallback() {
			@Override
			public void onConnect(SocketChannel socketChannel) {
				StatusListener statusListener = new StatusListener() {
					@Override
					public void onOpen(RpcClientConnection connection) {
						addConnection(address, connection);
					}

					@Override
					public void onClosed() {
						logger.info("Connection to {} closed", address);
						removeConnection(address);

						// jmx
						generalConnectsStats.getClosedConnects().recordEvent();
						connectsStatsPerAddress.get(address).getClosedConnects().recordEvent();

						connect(address);
					}
				};
				RpcClientConnection connection = new RpcClientConnectionImpl(eventloop, socketChannel,
						serializer.createSerializer(), protocolFactory, statusListener);
				connection.getSocketConnection().register();

				// jmx
				generalConnectsStats.getSuccessfulConnects().recordEvent();
				connectsStatsPerAddress.get(address).getSuccessfulConnects().recordEvent();

				logger.info("Connection to {} established", address);
				if (startCallback != null) {
					postCompletion(eventloop, startCallback);
					startCallback = null;
				}
			}

			@Override
			public void onException(Exception exception) {
				//jmx
				generalConnectsStats.getFailedConnects().recordEvent();
				connectsStatsPerAddress.get(address).getFailedConnects().recordEvent();

				if (running) {
					if (logger.isWarnEnabled()) {
						logger.warn("Connection failed, reconnecting to {}: {}", address, exception.toString());
					}
					eventloop.scheduleBackground(eventloop.currentTimeMillis() + reconnectIntervalMillis, new Runnable() {
						@Override
						public void run() {
							if (running) {
								connect(address);
							}
						}
					});
				}
			}
		});
	}

	private void addConnection(InetSocketAddress address, RpcClientConnection connection) {
		connections.put(address, connection);

		// jmx
		if (isMonitoring()) {
			if (connection instanceof RpcJmxClientConnection)
				((RpcJmxClientConnection) connection).startMonitoring();
		}

		RpcSender sender = strategy.createSender(pool);
		requestSender = sender != null ? sender : new Sender();
	}

	private void removeConnection(InetSocketAddress address) {
		connections.remove(address);
		RpcSender sender = strategy.createSender(pool);
		requestSender = sender != null ? sender : new Sender();
	}

	private void closeConnections() {
		for (RpcClientConnection connection : new ArrayList<>(connections.values())) {
			connection.close();
		}
	}

	public <T> void sendRequest(Object request, int timeout, ResultCallback<T> callback) {
		ResultCallback<T> requestCallback = callback;

		// jmx
		generalRequestsStats.getTotalRequests().recordEvent();
		if (isMonitoring()) {
			Class<?> requestClass = request.getClass();
			ensureRequestStatsSetPerClass(requestClass).getTotalRequests().recordEvent();
			requestCallback = new JmxMonitoringResultCallback<>(requestClass, callback);
		}

		requestSender.sendRequest(request, timeout, requestCallback);

	}

	public <T> ResultCallbackFuture<T> sendRequestFuture(final Object request, final int timeout) {
		final ResultCallbackFuture<T> future = new ResultCallbackFuture<>();
		eventloop.postConcurrently(new Runnable() {
			@Override
			public void run() {
				sendRequest(request, timeout, future);
			}
		});
		return future;
	}

	// visible for testing
	public RpcSender getRequestSender() {
		return requestSender;
	}

	static final class Sender implements RpcSender {
		@SuppressWarnings("ThrowableInstanceNeverThrown")
		private static final RpcNoSenderException NO_SENDER_AVAILABLE_EXCEPTION
				= new RpcNoSenderException("No senders available");

		@Override
		public <I, O> void sendRequest(I request, int timeout, ResultCallback<O> callback) {
			callback.onException(NO_SENDER_AVAILABLE_EXCEPTION);
		}
	}

	// JMX

	/**
	 * Thread-safe operation
	 */
	@Override
	public void startMonitoring() {
		eventloop.postConcurrently(new Runnable() {
			@Override
			public void run() {
				RpcClient.this.monitoring = true;
				for (InetSocketAddress address : addresses) {
					RpcClientConnection connection = pool.get(address);
					if (connection != null && connection instanceof RpcJmxClientConnection) {
						((RpcJmxClientConnection)(connection)).startMonitoring();
					}
				}
			}
		});
	}

	/**
	 * Thread-safe operation
	 */
	@Override
	public void stopMonitoring() {
		eventloop.postConcurrently(new Runnable() {
			@Override
			public void run() {
				RpcClient.this.monitoring = false;
				for (InetSocketAddress address : addresses) {
					RpcClientConnection connection = pool.get(address);
					if (connection != null && connection instanceof RpcJmxClientConnection) {
						((RpcJmxClientConnection)(connection)).stopMonitoring();
					}
				}
			}
		});
	}

	private boolean isMonitoring() {
		return monitoring;
	}

	/**
	 * Thread-safe operation
	 */
	@Override
	public void reset() {
		eventloop.postConcurrently(new Runnable() {
			@Override
			public void run() {
				resetStats(smoothingWindow, smoothingPrecision);
			}
		});
	}

	/**
	 * Thread-safe operation
	 */
	@Override
	public void reset(final double smoothingWindow, final double smoothingPrecision) {
		eventloop.postConcurrently(new Runnable() {
			@Override
			public void run() {
				RpcClient.this.smoothingWindow = smoothingWindow;
				RpcClient.this.smoothingPrecision = smoothingPrecision;
				resetStats(smoothingWindow, smoothingPrecision);
			}
		});
	}

	private void resetStats(double smoothingWindow, double smoothingPrecision) {
		generalRequestsStats.reset(smoothingWindow, smoothingPrecision);
		for (InetSocketAddress address : connectsStatsPerAddress.keySet()) {
			connectsStatsPerAddress.get(address).reset(smoothingWindow, smoothingPrecision);
		}
		for (Class<?> requestClass : requestStatsPerClass.keySet()) {
			requestStatsPerClass.get(requestClass).reset(smoothingWindow, smoothingPrecision);
		}
		for (InetSocketAddress address : addresses) {
			RpcClientConnection connection = pool.get(address);
			if (connection != null && connection instanceof RpcJmxClientConnection) {
				((RpcJmxClientConnection)(connection)).reset(smoothingWindow, smoothingPrecision);
			}
		}
	}

	/**
	 * Thread-safe operation
	 */
	@Override
	public void getGeneralRequestsStats(final BlockingQueue<RpcJmxRequestsStatsSet> container) {
		eventloop.postConcurrently(new Runnable() {
			@Override
			public void run() {
				try {
					container.add(generalRequestsStats);
				} catch (Exception e) {
					// TODO(vmykhalko): is this logging level correct ?
					logger.warn("Cannot add stats to blocking queue for jmx using", e);
				}
			}
		});
	}

	/**
	 * Thread-safe operation
	 */
	@Override
	public void getRequestsStatsPerClass(final BlockingQueue<Map<Class<?>, RpcJmxRequestsStatsSet>> container) {
		eventloop.postConcurrently(new Runnable() {
			@Override
			public void run() {
				try {
					container.add(requestStatsPerClass);
				} catch (Exception e) {
					// TODO(vmykhalko): is this logging level correct ?
					logger.warn("Cannot add stats to blocking queue for jmx using", e);
				}
			}
		});
	}

	/**
	 * Thread-safe operation
	 */
	@Override
	public void getConnectsStats(final BlockingQueue<Map<InetSocketAddress, RpcJmxConnectsStatsSet>> container) {
		eventloop.postConcurrently(new Runnable() {
			@Override
			public void run() {
				try {
					container.add(connectsStatsPerAddress);
				} catch (Exception e) {
					// TODO(vmykhalko): is this logging level correct ?
					logger.warn("Cannot add stats to blocking queue for jmx using", e);
				}
			}
		});
	}

	/**
	 * Thread-safe operation
	 */
	@Override
	public void getRequestStatsPerAddress(final BlockingQueue<Map<InetSocketAddress, RpcJmxRequestsStatsSet>> container) {
		eventloop.postConcurrently(new Runnable() {
			@Override
			public void run() {
				try {
					Map<InetSocketAddress, RpcJmxRequestsStatsSet> requestStatsPerAddress = new HashMap<>();
					for (InetSocketAddress address : addresses) {
						RpcClientConnection connection = pool.get(address);
						if (connection != null && connection instanceof RpcJmxClientConnection) {
							RpcJmxRequestsStatsSet stats = ((RpcJmxClientConnection) (connection)).getRequestStats();
							requestStatsPerAddress.put(address, stats);
						}
					}
					container.add(requestStatsPerAddress);
				} catch (Exception e) {
					// TODO(vmykhalko): is this logging level correct ?
					logger.warn("Cannot add stats to blocking queue for jmx using", e);
				}
			}
		});
	}

	private RpcJmxRequestsStatsSet ensureRequestStatsSetPerClass(Class<?> requestClass) {
		if (!requestStatsPerClass.containsKey(requestClass)) {
			requestStatsPerClass.put(requestClass,
					new RpcJmxRequestsStatsSet(smoothingWindow, smoothingPrecision, eventloop));
		}
		return requestStatsPerClass.get(requestClass);
	}

	private final class JmxMonitoringResultCallback<T> implements ResultCallback<T> {

		private Stopwatch stopwatch;
		private final Class<?> requestClass;
		private final ResultCallback<T> callback;

		public JmxMonitoringResultCallback(Class<?> requestClass, ResultCallback<T> callback) {
			this.stopwatch = Stopwatch.createStarted();
			this.requestClass = requestClass;
			this.callback = callback;
		}

		@Override
		public void onResult(T result) {
			if (isMonitoring()) {
				generalRequestsStats.getSuccessfulRequests().recordEvent();
				ensureRequestStatsSetPerClass(requestClass).getSuccessfulRequests().recordEvent();
				updateResponseTime(requestClass, timeElapsed());
			}
			callback.onResult(result);
		}

		@Override
		public void onException(Exception exception) {
			if (isMonitoring()) {
				if (exception instanceof RpcTimeoutException) {
					generalRequestsStats.getExpiredRequests().recordEvent();
					ensureRequestStatsSetPerClass(requestClass).getExpiredRequests().recordEvent();
				} else if (exception instanceof RpcOverloadException) {
					generalRequestsStats.getRejectedRequests().recordEvent();
					ensureRequestStatsSetPerClass(requestClass).getRejectedRequests().recordEvent();
				} else if (exception instanceof RpcRemoteException) {
					generalRequestsStats.getFailedRequests().recordEvent();
					ensureRequestStatsSetPerClass(requestClass).getFailedRequests().recordEvent();
					updateResponseTime(requestClass, timeElapsed());

					long timestamp = eventloop.currentTimeMillis();
					// TODO(vmykhalko): maybe there should be something more informative instead of null (as causedObject)?
					generalRequestsStats.getLastServerExceptionCounter().update(exception, null, timestamp);
					ensureRequestStatsSetPerClass(requestClass)
							.getLastServerExceptionCounter().update(exception, null, timestamp);
				}
			}
			callback.onException(exception);
		}

		private void updateResponseTime(Class<?> requestClass, int responseTime) {
			generalRequestsStats.getResponseTimeStats().recordValue(responseTime);
			ensureRequestStatsSetPerClass(requestClass).getResponseTimeStats().recordValue(responseTime);
		}

		private int timeElapsed() {
			return (int)(stopwatch.elapsed(TimeUnit.MILLISECONDS));
		}
	}
}

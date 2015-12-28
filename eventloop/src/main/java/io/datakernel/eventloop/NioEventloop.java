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

import io.datakernel.annotation.Nullable;
import io.datakernel.async.ResultCallbackFuture;
import io.datakernel.eventloop.jmx.NioEventloopJmx;
import io.datakernel.eventloop.jmx.NioEventloopStatsSet;
import io.datakernel.jmx.LastExceptionCounter;
import io.datakernel.net.DatagramSocketSettings;
import io.datakernel.net.ServerSocketSettings;
import io.datakernel.net.SocketSettings;
import io.datakernel.time.CurrentTimeProvider;
import io.datakernel.time.CurrentTimeProviderSystem;
import io.datakernel.util.ExceptionMarker;
import io.datakernel.util.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.*;
import java.nio.channels.spi.SelectorProvider;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static io.datakernel.jmx.MBeanUtils.register;

/**
 * It is internal class for asynchronous programming. NioEventloop represents infinite loop with only one
 * blocking operation selector.select() which selects a set of keys whose corresponding channels are
 * ready for I/O operations. With this keys and queues with tasks, which was added to Eventloop
 * from the outside, it begins asynchronous executing from one thread it in method run() which is overridden
 * because it is implementation of {@link Runnable}. Working of this eventloop will be ended, when it has
 * not selected keys and its queues with tasks are empty.
 */
public final class NioEventloop implements Eventloop, Runnable, NioEventloopJmx {
	private static final Logger logger = LoggerFactory.getLogger(NioEventloop.class);

	private static final TimeoutException CONNECT_TIMEOUT = new TimeoutException("Connection timed out");
	private static final long DEFAULT_EVENT_TIMEOUT = 20L;

	/**
	 * Collection of local tasks which was added from this thread.
	 */
	private final ArrayDeque<Runnable> localTasks = new ArrayDeque<>();

	/**
	 * Collection of concurrent tasks which was added from other threads.
	 */
	private final ConcurrentLinkedQueue<Runnable> concurrentTasks = new ConcurrentLinkedQueue<>();

	/**
	 * Collection of scheduled tasks that are scheduled at particular timestamp.
	 */
	private final PriorityQueue<ScheduledRunnable> scheduledTasks = new PriorityQueue<>();

	/**
	 * Collection of background tasks,
	 * which mean that if eventloop contains only background tasks, it will be closed
	 */
	private final PriorityQueue<ScheduledRunnable> backgroundTasks = new PriorityQueue<>();

	/**
	 * Count of concurrent operations in other threads, non-zero value prevents event loop from termination.
	 */
	private final AtomicInteger concurrentOperationsCount = new AtomicInteger(0);

	private final Map<Class<?>, Object> localMap = new HashMap<>();

	private final CurrentTimeProvider timeProvider;

	private long timeBeforeSelectorSelect;
	private long timeAfterSelectorSelect;

	/**
	 * The NIO selector which selects a set of keys whose corresponding channels
	 */
	private Selector selector;

	/**
	 * The thread where eventloop is running
	 */
	private Thread eventloopThread;
	/**
	 * The desired name of the thread
	 */
	private String threadName;

	private volatile boolean keepAlive;
	private volatile boolean breakEventloop;

	private long tick;

	/**
	 * Current time, cached to avoid System.currentTimeMillis() system calls, and to facilitate unit testing.
	 * It is being refreshed with each event loop execution.
	 */
	private long timestamp;

	public ThrottlingController throttlingController;

	/**
	 * Count of selected keys for last Selector.select()
	 */
	private int lastSelectedKeys;

	// JMX
	private static final double DEFAULT_SMOOTHING_WINDOW = 10.0; // 10 seconds
	private static final double DEFAULT_SMOOTHING_PRECISION = 0.1; // 0.1 seconds

	private static final ExceptionMarker ACCEPT_MARKER = NioEventloopStatsSet.exceptionMarker(NioEventloop.class, "AcceptException");
	private static final ExceptionMarker CONNECT_MARKER = NioEventloopStatsSet.exceptionMarker(NioEventloop.class, "ConnectException");
	private static final ExceptionMarker CONNECT_TIMEOUT_MARKER = NioEventloopStatsSet.exceptionMarker(NioEventloop.class, "ConnectTimeout");
	private static final ExceptionMarker READ_MARKER = NioEventloopStatsSet.exceptionMarker(NioEventloop.class, "ReadException");
	private static final ExceptionMarker WRITE_MARKER = NioEventloopStatsSet.exceptionMarker(NioEventloop.class, "WriteException");
	private static final ExceptionMarker CLOSE_MARKER = NioEventloopStatsSet.exceptionMarker(NioEventloop.class, "CloseException");
	private static final ExceptionMarker LOCAL_TASK_MARKER = NioEventloopStatsSet.exceptionMarker(NioEventloop.class, "LocalTaskException");
	private static final ExceptionMarker CONCURRENT_TASK_MARKER = NioEventloopStatsSet.exceptionMarker(NioEventloop.class, "ConcurrentTaskException");
	private static final ExceptionMarker SCHEDULED_TASK_MARKER = NioEventloopStatsSet.exceptionMarker(NioEventloop.class, "ScheduledTaskException");
	private static final ExceptionMarker UNCHECKED_MARKER = NioEventloopStatsSet.exceptionMarker(NioEventloop.class, "UncheckedException");

	private final NioEventloopStatsSet statsSet;

	private boolean monitoring;

	/**
	 * Creates a new instance of Eventloop with default instance of ByteBufPool
	 */
	public NioEventloop() {
		this(CurrentTimeProviderSystem.instance());
	}

	/**
	 * Creates a new instance of Eventloop with given ByteBufPool and timeProvider
	 *
	 * @param timeProvider provider for retrieving time on each cycle of event loop. Useful for unit testing.
	 */
	public NioEventloop(CurrentTimeProvider timeProvider) {
		this.timeProvider = timeProvider;
		refreshTimestampAndGet();
		this.statsSet = new NioEventloopStatsSet(DEFAULT_SMOOTHING_WINDOW, DEFAULT_SMOOTHING_PRECISION, this);
	}

	private void openSelector() {
		if (selector == null) {
			try {
				selector = SelectorProvider.provider().openSelector();
			} catch (Exception exception) {
				logger.error("Could not open selector", exception);
				throw new RuntimeException(exception);
			}
		}
	}

	/**
	 * Closes the selector if it has been opened.
	 */
	private void closeSelector() {
		if (selector != null) {
			try {
				selector.close();
				selector = null;
			} catch (Exception exception) {
				logger.error("Could not close selector", exception);
				throw new RuntimeException(exception);
			}
		}
	}

	Selector ensureSelector() {
		if (selector == null) {
			openSelector();
		}
		return selector;
	}

	public boolean inEventloopThread() {
		return eventloopThread == null || eventloopThread == Thread.currentThread();
	}

	/**
	 * Sets the flag keep alive, if it is true it means that working of this Eventloop will be
	 * continued even in case when all tasks have been executed and it doesn't have selected keys.
	 *
	 * @param keepAlive flag for setting
	 */
	public void keepAlive(boolean keepAlive) {
		this.keepAlive = keepAlive;
	}

	public void breakEventloop() {
		this.breakEventloop = true;
	}

	private boolean isKeepAlive() {
		if (breakEventloop)
			return false;
		return !localTasks.isEmpty() || !scheduledTasks.isEmpty() || !concurrentTasks.isEmpty()
				|| concurrentOperationsCount.get() > 0
				|| keepAlive || !selector.keys().isEmpty();
	}

	/**
	 * Sets the desired name of the thread
	 */
	public void setThreadName(String threadName) {
		this.threadName = threadName;
		if (eventloopThread != null)
			eventloopThread.setName(threadName);
	}

	/**
	 * Overridden method from Runnable that executes tasks while this eventloop is alive.
	 */
	@Override
	public void run() {
		eventloopThread = Thread.currentThread();
		if (threadName != null)
			eventloopThread.setName(threadName);
		ensureSelector();
		breakEventloop = false;

		timeBeforeSelectorSelect = timeAfterSelectorSelect = 0;
		while (isKeepAlive()) {
			tick++;

			try {
				updateBusinessLogicTimeStats();

				selector.select(getSelectTimeout());
				updateSelectorSelectTimeStats();

				processSelectedKeys(selector.selectedKeys());
				executeConcurrentTasks();
				executeScheduledTasks();
				executeBackgroundTasks();
				executeLocalTasks();
			} catch (Exception e) {
				updateExceptionCounter(UNCHECKED_MARKER, e, selector);
				logger.error("Exception in dispatch loop", e);
			}
		}

		eventloopThread = null;
		if (selector.keys().isEmpty()) {
			closeSelector();
			logger.trace("End of event loop {}", this);
		} else {
			logger.warn("Selector is still open, because event loop {} has {} keys", this, selector.keys());
		}
	}

	private void updateBusinessLogicTimeStats() {
		timeBeforeSelectorSelect = refreshTimestampAndGet();
		if (timeAfterSelectorSelect != 0) {
			long businessLogicTime = timeBeforeSelectorSelect - timeAfterSelectorSelect;
			if (throttlingController != null) {
				throttlingController.updateStats(lastSelectedKeys, (int) businessLogicTime);
			}
			statsSet.updateBusinessLogicTime(timeBeforeSelectorSelect, businessLogicTime);
		}
	}

	private void updateSelectorSelectTimeStats() {
		timeAfterSelectorSelect = refreshTimestampAndGet();
		long selectorSelectTime = timeAfterSelectorSelect - timeBeforeSelectorSelect;
		statsSet.updateSelectorSelectTime(selectorSelectTime);
	}

	private long getSelectTimeout() {
		if (!concurrentTasks.isEmpty())
			return 1L;
		if (scheduledTasks.isEmpty() && backgroundTasks.isEmpty())
			return DEFAULT_EVENT_TIMEOUT;
		long timeout = Math.min(getTimeBeforeExecution(scheduledTasks), getTimeBeforeExecution(backgroundTasks));
		if (timeout < 1L)
			return 1L;
		return Math.min(DEFAULT_EVENT_TIMEOUT, timeout);
	}

	private long getTimeBeforeExecution(PriorityQueue<ScheduledRunnable> taskQueue) {
		while (!taskQueue.isEmpty()) {
			ScheduledRunnable first = taskQueue.peek();
			assert first != null; // unreachable condition
			if (first.isCancelled()) {
				taskQueue.poll();
				continue;
			}
			return first.getTimestamp() - currentTimeMillis();
		}
		return DEFAULT_EVENT_TIMEOUT;
	}

	/**
	 * Processes selected keys related to various I/O events: accept, connect, read, write.
	 *
	 * @param selectedKeys set that contains all selected keys, returned from NIO Selector.select()
	 */
	private void processSelectedKeys(Set<SelectionKey> selectedKeys) {
		int invalidKeys = 0, acceptKeys = 0, connectKeys = 0, readKeys = 0, writeKeys = 0;

		lastSelectedKeys = selectedKeys.size();
		if (throttlingController != null) {
			throttlingController.calculateThrottling(lastSelectedKeys);
		}

		Stopwatch sw = isMonitoring() ? Stopwatch.createStarted() : null;

		Iterator<SelectionKey> iterator = selectedKeys.iterator();
		while (iterator.hasNext()) {
			SelectionKey key = iterator.next();
			iterator.remove();

			if (!key.isValid()) {
				invalidKeys++;
				continue;
			}

			if (key.isAcceptable()) {
				onAccept(key);
				acceptKeys++;
			} else if (key.isConnectable()) {
				onConnect(key);
				connectKeys++;
			} else {
				if (key.isReadable()) {
					onRead(key);
					readKeys++;
				}
				if (key.isValid()) {
					if (key.isWritable()) {
						onWrite(key);
						writeKeys++;
					}
				} else {
					invalidKeys++;
				}
			}
		}
		statsSet.updateSelectedKeysTimeStats(sw);
		statsSet.updateSelectedKeysStats(lastSelectedKeys, invalidKeys, acceptKeys, connectKeys, readKeys, writeKeys);
	}

	/**
	 * Executes local tasks which were added from current thread
	 */
	private void executeLocalTasks() {
		int newRunnables = 0;

		Stopwatch swTotal = isMonitoring() ? Stopwatch.createStarted() : null;
		Stopwatch sw = isMonitoring() ? Stopwatch.createUnstarted() : null;

		while (true) {
			Runnable item = localTasks.poll();
			if (item == null) {
				break;
			}

			if (sw != null) {
				sw.reset();
				sw.start();
			}

			try {
				item.run();
				if (sw != null)
					statsSet.updateLocalTaskDuration(item, sw);
			} catch (Throwable e) {
				if (sw != null)
					statsSet.updateLocalTaskDuration(item, sw);
				updateExceptionCounter(LOCAL_TASK_MARKER, e, item);
				logger.error(LOCAL_TASK_MARKER.getMarker(), "Exception thrown while execution Local-task", e);
			}
			newRunnables++;
		}
		statsSet.updateLocalTasksStats(newRunnables, swTotal);
	}

	/**
	 * Executes concurrent tasks which were added from other threads.
	 */
	private void executeConcurrentTasks() {
		int newRunnables = 0;

		Stopwatch swTotal = isMonitoring() ? Stopwatch.createStarted() : null;
		Stopwatch sw = isMonitoring() ? Stopwatch.createUnstarted() : null;

		while (true) {
			Runnable runnable = concurrentTasks.poll();
			if (runnable == null) {
				break;
			}

			if (sw != null) {
				sw.reset();
				sw.start();
			}

			try {
				runnable.run();
				if (sw != null)
					statsSet.updateConcurrentTaskDuration(runnable, sw);
			} catch (Throwable e) {
				if (sw != null)
					statsSet.updateConcurrentTaskDuration(runnable, sw);
				updateExceptionCounter(CONCURRENT_TASK_MARKER, e, runnable);
				logger.error(CONCURRENT_TASK_MARKER.getMarker(), "Exception in concurrent task {}", runnable, e);
			}
			newRunnables++;
		}
		statsSet.updateConcurrentTasksStats(newRunnables, swTotal);
	}

	/**
	 * Executes tasks, scheduled for execution at particular timestamps
	 */
	private void executeScheduledTasks() {
		executeScheduledTasks(scheduledTasks);
	}

	private void executeBackgroundTasks() {
		executeScheduledTasks(backgroundTasks);
	}

	private void executeScheduledTasks(PriorityQueue<ScheduledRunnable> taskQueue) {
		int newRunnables = 0;
		Stopwatch swTotal = isMonitoring() ? Stopwatch.createStarted() : null;
		Stopwatch sw = isMonitoring() ? Stopwatch.createUnstarted() : null;

		for (; ; ) {
			ScheduledRunnable peeked = taskQueue.peek();
			if (peeked == null)
				break;
			if (peeked.isCancelled()) {
				taskQueue.poll();
				continue;
			}
			if (peeked.getTimestamp() >= currentTimeMillis()) {
				break;
			}
			ScheduledRunnable polled = taskQueue.poll();
			assert polled == peeked;

			Runnable runnable = polled.getRunnable();
			if (sw != null) {
				sw.reset();
				sw.start();
			}

			try {
				runnable.run();
				polled.complete();
				if (sw != null)
					statsSet.updateScheduledTaskDuration(runnable, sw);
			} catch (Throwable e) {
				if (sw != null)
					statsSet.updateScheduledTaskDuration(runnable, sw);
				updateExceptionCounter(SCHEDULED_TASK_MARKER, e, polled);
				logger.error(SCHEDULED_TASK_MARKER.getMarker(), "Exception in Scheduled-task {}", runnable, e);
			}

			newRunnables++;
		}
		statsSet.updateScheduledTasksStats(newRunnables, swTotal);
	}

	/**
	 * Accepts an incoming socketChannel connections without blocking event loop thread.
	 *
	 * @param key key of this action.
	 */
	private void onAccept(SelectionKey key) {
		assert inEventloopThread();

		ServerSocketChannel channel = (ServerSocketChannel) key.channel();
		if (!channel.isOpen()) { // TODO - remove?
			key.cancel();
			return;
		}

		AcceptCallback acceptCallback = (AcceptCallback) key.attachment();
		for (; ; ) {
			SocketChannel socketChannel;
			try {
				socketChannel = channel.accept();
				if (socketChannel == null)
					break;
				socketChannel.configureBlocking(false);
			} catch (ClosedChannelException e) {
				break;
			} catch (Exception e) {
				updateExceptionCounter(ACCEPT_MARKER, e, channel);
				logger.error(ACCEPT_MARKER.getMarker(), "Could not finish accept to {}", channel, e.toString());
				break;
			}

			try {
				acceptCallback.onAccept(socketChannel);
			} catch (Exception e) {
				updateExceptionCounter(ACCEPT_MARKER, e, acceptCallback);
				logger.error(ACCEPT_MARKER.getMarker(), "onAccept exception {}", socketChannel, e);
				closeQuietly(socketChannel);
			}
		}
	}

	/**
	 * Processes newly established TCP connections without blocking event loop thread.
	 *
	 * @param key key of this action.
	 */
	private void onConnect(SelectionKey key) {
		assert inEventloopThread();
		ConnectCallback connectCallback = (ConnectCallback) key.attachment();
		SocketChannel socketChannel = (SocketChannel) key.channel();
		try {
			if (!socketChannel.finishConnect())
				return;
			connectCallback.onConnect(socketChannel);
		} catch (Exception e) {
			updateExceptionCounter(CONNECT_MARKER, e, connectCallback);
			logger.warn(CONNECT_MARKER.getMarker(), "Could not finish connect to {}", socketChannel, e);
			key.cancel();
			closeQuietly(socketChannel);
			connectCallback.onException(e);
		}
	}

	/**
	 * Processes socketChannels available for read, without blocking event loop thread.
	 *
	 * @param key key of this action.
	 */
	private void onRead(SelectionKey key) {
		assert inEventloopThread();
		SocketConnection connection = (SocketConnection) key.attachment();
		try {
			connection.onReadReady();
		} catch (Throwable e) {
			updateExceptionCounter(READ_MARKER, e, connection);
			logger.error(READ_MARKER.getMarker(), "Could not finish read to {}", connection, e);
		}
	}

	/**
	 * Processes socketChannels available for write, without blocking thread.
	 *
	 * @param key key of this action.
	 */
	private void onWrite(SelectionKey key) {
		assert inEventloopThread();
		SocketConnection connection = (SocketConnection) key.attachment();
		try {
			connection.onWriteReady();
		} catch (Throwable e) {
			updateExceptionCounter(WRITE_MARKER, e, connection);
			logger.error(WRITE_MARKER.getMarker(), "Could not finish write to {}", connection);
		}
	}

	private void closeQuietly(AutoCloseable closeable) {
		if (closeable == null)
			return;
		try {
			closeable.close();
		} catch (Exception e) {
			updateExceptionCounter(CLOSE_MARKER, e, closeable);
			if (logger.isWarnEnabled())
				logger.warn(CLOSE_MARKER.getMarker(), "Exception thrown while closing {}", closeable, e.toString());
		}
	}

	/**
	 * Creates {@link ServerSocketChannel} that listens on InetSocketAddress.
	 *
	 * @param address              InetSocketAddress that server will listen
	 * @param serverSocketSettings settings from this server channel
	 * @param acceptCallback       callback that will be called when new incoming connection is being accepted. It can be called multiple times.
	 * @return server channel
	 * @throws IOException If some  I/O error occurs
	 */
	public ServerSocketChannel listen(InetSocketAddress address, ServerSocketSettings serverSocketSettings, AcceptCallback acceptCallback) throws IOException {
		assert inEventloopThread();
		ServerSocketChannel serverChannel = null;
		try {
			serverChannel = ServerSocketChannel.open();
			serverSocketSettings.applySettings(serverChannel);
			serverChannel.configureBlocking(false);
			serverChannel.bind(address, serverSocketSettings.getBacklog());
			serverChannel.register(ensureSelector(), SelectionKey.OP_ACCEPT, acceptCallback);
			return serverChannel;
		} catch (IOException e) {
			closeQuietly(serverChannel);
			updateExceptionCounter(ACCEPT_MARKER, e, address);
			logger.warn(ACCEPT_MARKER.getMarker(), "Listen error for {}", address, e);
			throw e;
		}
	}

	/**
	 * Registers new UDP connection in this eventloop.
	 *
	 * @param bindAddress address for binding DatagramSocket for this connection.
	 * @return DatagramSocket of this connection
	 * @throws IOException if an I/O error occurs on opening DatagramChannel
	 */
	public static DatagramChannel createDatagramChannel(DatagramSocketSettings datagramSocketSettings,
	                                                    @Nullable InetSocketAddress bindAddress,
	                                                    @Nullable InetSocketAddress connectAddress) throws IOException {
		DatagramChannel datagramChannel = null;
		try {
			datagramChannel = DatagramChannel.open();
			datagramSocketSettings.applySettings(datagramChannel);
			datagramChannel.configureBlocking(false);
			datagramChannel.bind(bindAddress);
			if (connectAddress != null) {
				datagramChannel.connect(connectAddress);
			}
			return datagramChannel;
		} catch (IOException e) {
			if (datagramChannel != null) {
				try {
					datagramChannel.close();
				} catch (Exception ignored) {
				}
			}
			throw e;
		}
	}

	/**
	 * Connects to given socket address asynchronously.
	 *
	 * @param address         socketChannel's address
	 * @param socketSettings  socket's settings
	 * @param connectCallback callback for connecting, it will be called once when connection is established or failed.
	 */
	public void connect(SocketAddress address, SocketSettings socketSettings, ConnectCallback connectCallback) {
		connect(address, socketSettings, 0, connectCallback);
	}

	/**
	 * Connects to given socket address asynchronously with a specified timeout value.
	 * A timeout of zero is interpreted as an default system timeout
	 *
	 * @param address         socketChannel's address
	 * @param socketSettings  socket's settings
	 * @param timeout         the timeout value to be used in milliseconds, 0 as default system connection timeout
	 * @param connectCallback callback for connecting, it will be called once when connection is established or failed.
	 */
	public void connect(SocketAddress address, SocketSettings socketSettings, int timeout, ConnectCallback connectCallback) {
		assert inEventloopThread();
		SocketChannel socketChannel = null;
		try {
			socketChannel = SocketChannel.open();
			socketChannel.configureBlocking(false);
			socketSettings.applySettings(socketChannel);
			socketChannel.connect(address);
			socketChannel.register(ensureSelector(), SelectionKey.OP_CONNECT,
					timeoutConnectCallback(socketChannel, timeout, connectCallback));
		} catch (IOException e) {
			closeQuietly(socketChannel);
			updateExceptionCounter(CONNECT_MARKER, e, address);
			logger.warn(CONNECT_MARKER.getMarker(), "Connect error for {}", address, e);
			connectCallback.onException(e);
		}
	}

	/**
	 * Returns modified connectCallback to enable timeout.
	 * If connectionTime is zero, method returns input connectCallback.
	 * Otherwise schedules special task that will close SocketChannel and call onException method in case of timeout.
	 * If there is no timeout before connection - onConnect method will be called
	 */
	private ConnectCallback timeoutConnectCallback(final SocketChannel socketChannel, final long connectionTime, final ConnectCallback connectCallback) {
		if (connectionTime == 0)
			return connectCallback;

		return new ConnectCallback() {
			private final ScheduledRunnable scheduledTimeout = schedule(currentTimeMillis() + connectionTime, new Runnable() {
				@Override
				public void run() {
					updateExceptionCounter(CONNECT_TIMEOUT_MARKER, CONNECT_TIMEOUT, socketChannel.toString());
					logger.warn(CONNECT_TIMEOUT_MARKER.getMarker(), "Connection timed out for {}", socketChannel, CONNECT_TIMEOUT);
					closeQuietly(socketChannel);
					connectCallback.onException(CONNECT_TIMEOUT);
				}
			});

			@Override
			public void onConnect(SocketChannel socketChannel) {
				if (scheduledTimeout.isComplete())
					return;
				scheduledTimeout.cancel();
				connectCallback.onConnect(socketChannel);
			}

			@Override
			public void onException(Exception exception) {
				if (scheduledTimeout.isComplete())
					return;
				scheduledTimeout.cancel();
				connectCallback.onException(exception);
			}
		};
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T get(Class<T> type) {
		assert inEventloopThread();
		return (T) localMap.get(type);
	}

	@Override
	public <T> void set(Class<T> type, T value) {
		assert inEventloopThread();
		localMap.put(type, value);
	}

	@Override
	public long tick() {
		assert inEventloopThread();
		return tick;
	}

	/**
	 * Posts a new task to the beginning of localTasks.
	 * This method is recommended, since task will be executed as soon as possible without invalidating CPU cache.
	 *
	 * @param runnable runnable of this task
	 */
	@Override
	public void post(Runnable runnable) {
		assert inEventloopThread();
		localTasks.addFirst(runnable);
	}

	/**
	 * Posts a new task to the end localTasks.
	 *
	 * @param runnable runnable of this task
	 */
	@Override
	public void postLater(Runnable runnable) {
		assert inEventloopThread();
		localTasks.addLast(runnable);
	}

	/**
	 * Posts a new task from other threads.
	 * This is the preferred method of communicating with eventloop from another threads.
	 *
	 * @param runnable runnable of this task
	 */
	@Override
	public void postConcurrently(Runnable runnable) {
		concurrentTasks.offer(runnable);
		if (selector != null) {
			selector.wakeup();
		}
	}

	/**
	 * Posts from another thread task, which is expected to return result.
	 * <p/>
	 * This is preferred method to communicate with eventloop from another thread when a result is expected
	 *
	 * @param callable task to be executed
	 * @param <V>      type of result
	 * @return {@code Future}, which can be used to retrieve result
	 */
	@Override
	public <V> Future<V> postConcurrently(final Callable<V> callable) {
		final ResultCallbackFuture<V> future = new ResultCallbackFuture<>();
		postConcurrently(new Runnable() {
			@Override
			public void run() {
				V result = null;
				Exception throwedException = null;

				try {
					result = callable.call();
				} catch (Exception e) {
					throwedException = e;
				}

				if (throwedException == null) {
					future.onResult(result);
				} else {
					future.onException(throwedException);
				}
			}
		});
		return future;
	}

	/**
	 * Schedules new task. Returns {@link ScheduledRunnable} with this runnable.
	 *
	 * @param timestamp timestamp after which task will be ran
	 * @param runnable  runnable of this task
	 * @return scheduledRunnable, which could used for cancelling the task
	 */
	@Override
	public ScheduledRunnable schedule(long timestamp, Runnable runnable) {
		assert inEventloopThread();
		return addScheduledTask(timestamp, runnable, false);
	}

	/**
	 * Schedules new background task. Returns {@link ScheduledRunnable} with this runnable.
	 * <p/>
	 * If eventloop contains only background tasks, it will be closed
	 *
	 * @param timestamp timestamp after which task will be ran
	 * @param runnable  runnable of this task
	 * @return scheduledRunnable, which could used for cancelling the task
	 */
	@Override
	public ScheduledRunnable scheduleBackground(long timestamp, Runnable runnable) {
		assert inEventloopThread();
		return addScheduledTask(timestamp, runnable, true);
	}

	private ScheduledRunnable addScheduledTask(long timestamp, Runnable runnable, boolean background) {
		ScheduledRunnable scheduledRunnable = new ScheduledRunnable(timestamp, runnable);
		PriorityQueue<ScheduledRunnable> taskQueue = background ? backgroundTasks : scheduledTasks;
		taskQueue.offer(scheduledRunnable);
		return scheduledRunnable;
	}

	/**
	 * Notifies the event loop of concurrent operation in another thread(s).
	 * Eventloop will not exit until all concurrent operations are complete.
	 *
	 * @return {@link ConnectionPendingException}, that have method complete() which must be
	 * called after completing concurrent operation.
	 * Failure to call complete() method will prevent the event loop from exiting.
	 */
	@Override
	public ConcurrentOperationTracker startConcurrentOperation() {
		concurrentOperationsCount.incrementAndGet();

		return new ConcurrentOperationTracker() {
			private final AtomicBoolean complete = new AtomicBoolean(false);

			@Override
			public void complete() {
				if (complete.compareAndSet(false, true)) {
					if (concurrentOperationsCount.decrementAndGet() < 0) {
						logger.error("Concurrent operations count < 0");
					}
				} else {
					logger.error("Concurrent operation is already complete");
				}
			}
		};
	}

	public long refreshTimestampAndGet() {
		timestamp = timeProvider.currentTimeMillis();
		return timestamp;
	}

	/**
	 * Returns current time of this eventloop
	 */
	@Override
	public long currentTimeMillis() {
		return timestamp;
	}

	// JMX

	@Override
	public void startMonitoring() {
		this.monitoring = true;
	}

	@Override
	public void stopMonitoring() {
		this.monitoring = false;
	}

	@Override
	public boolean isMonitoring() {
		return monitoring;
	}

	@Override
	public void resetStats() {
		statsSet.resetStats();
	}

	@Override
	public void resetStats(double smoothingWindow, double smoothingPrecision) {
		statsSet.resetStats(smoothingWindow, smoothingPrecision);
	}

	/**
	 * Thread-safe operation
	 * @return
	 */
	@Override
	public NioEventloopStatsSet getStatsSet() {
		try {
			return postConcurrently(new Callable<NioEventloopStatsSet>() {
				@Override
				public NioEventloopStatsSet call() throws Exception {
					return statsSet;
				}
			}).get();
		} catch (InterruptedException | ExecutionException e) {
			throw new RuntimeException(e);
		}
	}

	public LastExceptionCounter getExceptionCounter(ExceptionMarker marker) {
		return statsSet.getExceptionCounter(marker);
	}

	public void updateExceptionCounter(ExceptionMarker marker, Throwable e, Object o) {
		assert inEventloopThread();
		statsSet.updateExceptionCounter(marker, e, o, currentTimeMillis());
	}

	public void resetExceptionCounter(ExceptionMarker marker) {
		assert inEventloopThread();
		statsSet.resetExceptionCounter(marker);
	}
}

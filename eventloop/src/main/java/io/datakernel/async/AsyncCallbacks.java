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

package io.datakernel.async;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopServer;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.util.Function;

import java.io.IOException;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Static utility methods pertaining to {@link ResultCallback}, {@link CompletionCallback} and working with
 * {@link EventloopServer} and {@link EventloopService}.
 */
public final class AsyncCallbacks {
	private AsyncCallbacks() {

	}

	/**
	 * Returns CompletionCallback, which no reaction on its callings.
	 */
	public static CompletionCallback ignoreCompletionCallback() {
		return new CompletionCallback() {
			@Override
			protected void onComplete() { }

			@Override
			protected void onException(Exception exception) { }
		};
	}

	/**
	 * Returns ResultCallback, which no reaction on its callings.
	 */
	@SuppressWarnings("unchecked")
	public static <T> ResultCallback<T> ignoreResultCallback() {
		return (ResultCallback<T>) new ResultCallback<Object>() {
			@Override
			protected void onResult(Object result) { }

			@Override
			protected void onException(Exception exception) { }
		};
	}

	private static final AsyncCancellable NOT_CANCELLABLE = new AsyncCancellable() {
		@Override
		public void cancel() {
			// Do nothing
		}
	};

	public static AsyncCancellable notCancellable() {
		return NOT_CANCELLABLE;
	}

	/**
	 * Posts sendResult()
	 *
	 * @param eventloop event loop in which it will call callback
	 * @param callback  the callback for calling
	 * @param result    the result with which ResultCallback will be called.
	 * @param <T>       type of result
	 */
	public static <T> void postResult(Eventloop eventloop, final ResultCallback<T> callback, final T result) {
		eventloop.post(new Runnable() {
			@Override
			public void run() {
				callback.sendResult(result);
			}
		});
	}

	/**
	 * Posts fireException()
	 *
	 * @param eventloop event loop in which it will call callback
	 * @param callback  the callback for calling
	 * @param e         the exception with which ResultCallback will be called.
	 */
	public static void postException(Eventloop eventloop, final ExceptionCallback callback, final Exception e) {
		eventloop.post(new Runnable() {
			@Override
			public void run() {
				callback.fireException(e);
			}
		});
	}

	/**
	 * Posts complete()
	 *
	 * @param eventloop event loop in which it will call callback
	 * @param callback  the callback for calling
	 */
	public static void postCompletion(Eventloop eventloop, final CompletionCallback callback) {
		eventloop.post(new Runnable() {
			@Override
			public void run() {
				callback.complete();
			}
		});
	}

	/**
	 * Posts sendNext() from IteratorCallback from arguments in event loop
	 *
	 * @param eventloop event loop in which it will call callback
	 * @param callback  the callback for calling
	 * @param next      the element with which IteratorCallback will be called.
	 * @param <T>       type of elements in iterator
	 */
	public static <T> void postNext(Eventloop eventloop, final IteratorCallback<T> callback, final T next) {
		eventloop.post(new Runnable() {
			@Override
			public void run() {
				callback.sendNext(next);
			}
		});
	}

	/**
	 * Calls end() from IteratorCallback from arguments in event loop from other thread
	 *
	 * @param eventloop event loop in which it will call callback
	 * @param callback  the callback for calling
	 * @param <T>       type of elements in iterator
	 */
	public static <T> void postEnd(Eventloop eventloop, final IteratorCallback<T> callback) {
		eventloop.post(new Runnable() {
			@Override
			public void run() {
				callback.end();
			}
		});
	}

	/**
	 * Calls sendResult() from ResultCallback from arguments in event loop from other thread.
	 *
	 * @param eventloop event loop in which it will call callback
	 * @param callback  the callback for calling
	 * @param result    the result with which ResultCallback will be called.
	 * @param <T>       type of result
	 */
	public static <T> void postResultConcurrently(Eventloop eventloop, final ResultCallback<T> callback, final T result) {
		eventloop.execute(new Runnable() {
			@Override
			public void run() {
				callback.sendResult(result);
			}
		});
	}

	/**
	 * Calls fireException() from other thread.
	 *
	 * @param eventloop event loop in which it will call callback
	 * @param callback  the callback for calling
	 * @param e         the exception with which ResultCallback will be called.
	 */
	public static void postExceptionConcurrently(Eventloop eventloop, final ExceptionCallback callback, final Exception e) {
		eventloop.execute(new Runnable() {
			@Override
			public void run() {
				callback.fireException(e);
			}
		});
	}

	/**
	 * Calls complete() from CompletionCallback from arguments in event loop from other thread.
	 *
	 * @param eventloop event loop in which it will call callback
	 * @param callback  the callback for calling
	 */
	public static void postCompletionConcurrently(Eventloop eventloop, final CompletionCallback callback) {
		eventloop.execute(new Runnable() {
			@Override
			public void run() {
				callback.complete();
			}
		});
	}

	/**
	 * Calls sendNext() from IteratorCallback from arguments in event loop from other thread
	 *
	 * @param eventloop event loop in which it will call callback
	 * @param callback  the callback for calling
	 * @param next      the element with which IteratorCallback will be called.
	 * @param <T>       type of elements in iterator
	 */
	public static <T> void postNextConcurrently(Eventloop eventloop, final IteratorCallback<T> callback, final T next) {
		eventloop.execute(new Runnable() {
			@Override
			public void run() {
				callback.sendNext(next);
			}
		});
	}

	/**
	 * Calls end() from IteratorCallback from arguments in event loop from other thread
	 *
	 * @param eventloop event loop in which it will call callback
	 * @param callback  the callback for calling
	 * @param <T>       type of elements in iterator
	 */
	public static <T> void postEndConcurrently(Eventloop eventloop, final IteratorCallback<T> callback) {
		eventloop.execute(new Runnable() {
			@Override
			public void run() {
				callback.end();
			}
		});
	}

	/**
	 * Returns new  AsyncTask which contains all tasks from argument and executes them successively
	 * through callback after calling execute()
	 *
	 * @param tasks asynchronous tasks for non-blocking running
	 */
	public static AsyncTask sequence(final AsyncTask... tasks) {
		return new AsyncTask() {
			@Override
			public void execute(final CompletionCallback callback) {
				if (tasks.length == 0) {
					callback.complete();
				} else {
					CompletionCallback internalCallback = new ForwardingCompletionCallback(callback) {
						int n = 1;

						@Override
						protected void onComplete() {
							if (n == tasks.length) {
								callback.complete();
							} else {
								AsyncTask task = tasks[n++];
								task.execute(this);
							}
						}
					};

					AsyncTask task = tasks[0];
					task.execute(internalCallback);
				}
			}
		};
	}

	/**
	 * Returns new  AsyncTask which contains two tasks from argument and executes them successively
	 * through callback after calling execute()
	 */
	public static AsyncTask sequence(AsyncTask task1, AsyncTask task2) {
		return sequence(new AsyncTask[]{task1, task2});
	}

	/**
	 * Returns new  AsyncTask which contains tasks from argument and executes them parallel
	 */
	public static AsyncTask parallel(final AsyncTask... tasks) {
		return new AsyncTask() {
			@Override
			public void execute(final CompletionCallback callback) {
				if (tasks.length == 0) {
					callback.complete();
				} else {
					CompletionCallback internalCallback = new ForwardingCompletionCallback(callback) {
						int n = tasks.length;

						@Override
						protected void onComplete() {
							if (--n == 0) {
								callback.complete();
							}
						}

						@Override
						protected void onException(Exception exception) {
							if (n > 0) {
								n = 0;
								callback.fireException(exception);
							}
						}
					};

					for (AsyncTask task : tasks) {
						task.execute(internalCallback);
					}
				}
			}
		};
	}

	/**
	 * Returns  AsyncGetter which parallel processed results from getters from argument
	 *
	 * @param returnOnFirstException flag that means that on first throwing exception in getters
	 *                               method should returns AsyncGetter
	 */
	public static AsyncGetter<Object[]> parallel(final boolean returnOnFirstException, final AsyncGetter<?>... getters) {
		return new AsyncGetter<Object[]>() {
			class Holder {
				int n = 0;
				Exception[] exceptions;
			}

			@SuppressWarnings("unchecked")
			@Override
			public void get(final ResultCallback<Object[]> callback) {
				final Object[] results = new Object[getters.length];
				if (getters.length == 0) {
					callback.sendResult(results);
				} else {
					final Holder holder = new Holder();
					holder.n = getters.length;
					for (int i = 0; i < getters.length; i++) {
						AsyncGetter<Object> getter = (AsyncGetter<Object>) getters[i];

						final int finalI = i;
						getter.get(new ForwardingResultCallback<Object>(callback) {
							private void checkCompleteResult() {
								if (--holder.n == 0) {
									if (holder.exceptions == null)
										callback.sendResult(results);
									else
										callback.fireException(new ParallelExecutionException(results, holder.exceptions));
								}
							}

							@Override
							protected void onResult(Object result) {
								results[finalI] = result;
								checkCompleteResult();
							}

							@Override
							protected void onException(Exception exception) {
								if (holder.exceptions == null) {
									holder.exceptions = new Exception[getters.length];
								}
								holder.exceptions[finalI] = exception;
								if (returnOnFirstException && holder.n > 0)
									holder.n = 1;
								checkCompleteResult();
							}
						});
					}

				}
			}
		};
	}

	/**
	 * Returns new  AsyncTask which contains two tasks from argument and executes them parallel
	 */
	public static AsyncTask parallel(AsyncTask task1, AsyncTask task2) {
		return parallel(new AsyncTask[]{task1, task2});
	}

	/**
	 * Returns new  AsyncFunction which contains functions from argument and applies them successively
	 *
	 * @param <I> type of value for input to function
	 * @param <O> type of result
	 */
	public static <I, O> AsyncFunction<I, O> sequence(final AsyncFunction<?, ?>... functions) {
		return new AsyncFunction<I, O>() {
			@SuppressWarnings("unchecked")
			@Override
			public void apply(I input, final ResultCallback<O> callback) {
				if (functions.length == 0) {
					callback.sendResult((O) input);
				} else {
					ForwardingResultCallback<Object> internalCallback = new ForwardingResultCallback<Object>(callback) {
						int n = 1;

						@Override
						protected void onResult(Object result) {
							if (n == functions.length) {
								callback.sendResult((O) result);
							} else {
								AsyncFunction<Object, Object> function = (AsyncFunction<Object, Object>) functions[n++];
								function.apply(result, this);
							}
						}
					};

					AsyncFunction<I, Object> function = (AsyncFunction<I, Object>) functions[0];
					function.apply(input, internalCallback);
				}
			}
		};
	}

	/**
	 * Returns new AsyncFunction which is composition from two functions from argument.
	 * Type of output first function should equals type for input for second function
	 *
	 * @param <F> type of input first function
	 * @param <T> type of output first function and input for second function
	 * @param <O> type of output second function
	 */
	public static <F, T, O> AsyncFunction<F, O> sequence(final AsyncFunction<F, T> function1, final AsyncFunction<T, O> function2) {
		return new AsyncFunction<F, O>() {
			@Override
			public void apply(F input, final ResultCallback<O> callback) {
				function1.apply(input, new ForwardingResultCallback<T>(callback) {
					@Override
					protected void onResult(T result) {
						function2.apply(result, new ForwardingResultCallback<O>(callback) {
							@Override
							protected void onResult(O result) {
								callback.sendResult(result);
							}
						});
					}
				});
			}
		};
	}

	/**
	 * Returns new AsyncTask which set value for AsyncSetter
	 *
	 * @param <T> type of value
	 */
	public static <T> AsyncTask combine(final T value, final AsyncSetter<T> setter) {
		return new AsyncTask() {
			@Override
			public void execute(CompletionCallback callback) {
				setter.set(value, callback);
			}
		};
	}

	/**
	 * Returns new AsyncGetter which with method get() applies from to function
	 *
	 * @param <F> type of value for appplying
	 * @param <T> type of output function
	 */
	public static <F, T> AsyncGetter<T> combine(final F from, final AsyncFunction<F, T> function) {
		return new AsyncGetter<T>() {
			@Override
			public void get(ResultCallback<T> callback) {
				function.apply(from, callback);
			}
		};
	}

	/**
	 * Returns getter which with method get() calls sendResult() with value with argument
	 *
	 * @param value value for argument sendResult()
	 * @param <T>   type of result
	 */
	public static <T> AsyncGetter<T> constGetter(final T value) {
		return new AsyncGetter<T>() {
			@Override
			public void get(ResultCallback<T> callback) {
				callback.sendResult(value);
			}
		};
	}

	public static <T> AsyncGetter<T> combine(AsyncTask asyncTask, T value) {
		return sequence(asyncTask, constGetter(value));
	}

	/**
	 * Returns AsyncTask which during executing calls method get() from getter and after that sets result to setter
	 *
	 * @param <T> type of result
	 */
	public static <T> AsyncTask sequence(final AsyncGetter<T> getter, final AsyncSetter<T> setter) {
		return new AsyncTask() {
			@Override
			public void execute(final CompletionCallback callback) {
				getter.get(new ForwardingResultCallback<T>(callback) {
					@Override
					protected void onResult(T result) {
						setter.set(result, callback);
					}
				});
			}
		};
	}

	/**
	 * Returns AsyncTask which during executing calls method get() from getter and after that calls sendResult() from resultCallback
	 *
	 * @param <T> type of result
	 */
	public static <T> AsyncTask combine(final AsyncGetter<T> getter, final ResultCallback<T> resultCallback) {
		return new AsyncTask() {
			@Override
			public void execute(final CompletionCallback callback) {
				getter.get(new ForwardingResultCallback<T>(callback) {
					@Override
					protected void onResult(T result) {
						resultCallback.sendResult(result);
						callback.complete();
					}

					@Override
					protected void onException(Exception exception) {
						resultCallback.fireException(exception);
						callback.fireException(exception);
					}
				});
			}
		};
	}

	public static <F, T> AsyncFunction<F, T> sequence(final AsyncSetter<F> setter, final AsyncGetter<T> getter) {
		return new AsyncFunction<F, T>() {
			@Override
			public void apply(F input, final ResultCallback<T> callback) {
				setter.set(input, new ForwardingCompletionCallback(callback) {
					@Override
					protected void onComplete() {
						getter.get(callback);
					}
				});
			}
		};
	}

	/**
	 * Returns AsyncGetter which executes AsyncTask from arguments and gets getter
	 *
	 * @param <T> type of result
	 */
	public static <T> AsyncGetter<T> sequence(final AsyncTask task, final AsyncGetter<T> getter) {
		return new AsyncGetter<T>() {
			@Override
			public void get(final ResultCallback<T> callback) {
				task.execute(new ForwardingCompletionCallback(callback) {
					@Override
					protected void onComplete() {
						getter.get(callback);
					}
				});
			}
		};
	}

	public static <T> AsyncSetter<T> sequence(final AsyncSetter<T> setter, final AsyncTask task) {
		return new AsyncSetter<T>() {
			@Override
			public void set(T value, final CompletionCallback callback) {
				setter.set(value, new ForwardingCompletionCallback(callback) {
					@Override
					protected void onComplete() {
						task.execute(callback);
					}
				});
			}
		};
	}

	public static <F, T> AsyncGetter<T> sequence(final AsyncGetter<F> getter, final AsyncFunction<F, T> function) {
		return new AsyncGetter<T>() {
			@Override
			public void get(final ResultCallback<T> callback) {
				getter.get(new ForwardingResultCallback<F>(callback) {
					@Override
					protected void onResult(F result) {
						function.apply(result, callback);
					}
				});
			}
		};
	}

	public static <F, T> AsyncGetter<T> combine(final AsyncGetter<F> getter, final Function<F, T> function) {
		return new AsyncGetter<T>() {
			@Override
			public void get(final ResultCallback<T> callback) {
				getter.get(new ForwardingResultCallback<F>(callback) {
					@Override
					protected void onResult(F result) {
						callback.sendResult(function.apply(result));
					}
				});
			}
		};
	}

	public static <F, T> AsyncSetter<F> sequence(final AsyncFunction<F, T> function, final AsyncSetter<T> setter) {
		return new AsyncSetter<F>() {
			@Override
			public void set(F value, final CompletionCallback callback) {
				function.apply(value, new ForwardingResultCallback<T>(callback) {
					@Override
					protected void onResult(T result) {
						setter.set(result, callback);
					}
				});
			}
		};
	}

	public static <F, T> AsyncSetter<F> combine(final Function<F, T> function, final AsyncSetter<T> setter) {
		return new AsyncSetter<F>() {
			@Override
			public void set(F value, CompletionCallback callback) {
				T to = function.apply(value);
				setter.set(to, callback);
			}
		};
	}

	public static <T> AsyncGetterWithSetter<T> createAsyncGetterWithSetter(Eventloop eventloop) {
		return new AsyncGetterWithSetter<>(eventloop);
	}

	public static final class WaitAllHandler {
		private final int minCompleted;
		private final int totalCount;
		private final CompletionCallback callback;

		private int completed = 0;
		private int exceptions = 0;
		private Exception lastException;

		private WaitAllHandler(int minCompleted, int totalCount, CompletionCallback callback) {
			this.minCompleted = minCompleted;
			this.totalCount = totalCount;
			this.callback = callback;
		}

		public CompletionCallback getCallback() {
			return new CompletionCallback() {
				@Override
				protected void onComplete() {
					++completed;
					completeResult();
				}

				@Override
				protected void onException(Exception exception) {
					++exceptions;
					lastException = exception;
					completeResult();
				}
			};
		}

		private void completeResult() {
			if ((exceptions + completed) == totalCount) {
				if (completed >= minCompleted) {
					callback.complete();
				} else {
					callback.fireException(lastException);
				}
			}
		}
	}

	/**
	 * Calls the callback from argument until number of callings it will be equal to count
	 *
	 * @param count    number of callings before running CompletionCallback
	 * @param callback CompletionCallback for calling
	 * @return new AsyncCompletionCallback which will be save count of callings
	 */
	public static WaitAllHandler waitAll(int count, CompletionCallback callback) {
		if (count == 0)
			callback.complete();

		return new WaitAllHandler(count, count, callback);
	}

	public static WaitAllHandler waitAll(int minCompleted, int totalCount, CompletionCallback callback) {
		if (totalCount == 0)
			callback.complete();

		return new WaitAllHandler(minCompleted, totalCount, callback);
	}

	/**
	 * Sets this NioServer as listen, sets future true if it was successfully, else sets exception which was
	 * threw.
	 *
	 * @param server the NioServer which it sets listen.
	 */
	public static CompletionCallbackFuture listenFuture(final EventloopServer server) {
		final CompletionCallbackFuture future = new CompletionCallbackFuture();
		server.getEventloop().execute(new Runnable() {
			@Override
			public void run() {
				try {
					server.listen();
					future.complete();
				} catch (IOException e) {
					future.fireException(e);
				}
			}
		});
		return future;
	}

	/**
	 * Closes this NioServer, sets future true if it was successfully.
	 *
	 * @param server the NioServer which it will close.
	 */
	public static CompletionCallbackFuture closeFuture(final EventloopServer server) {
		final CompletionCallbackFuture future = new CompletionCallbackFuture();
		server.getEventloop().execute(new Runnable() {
			@Override
			public void run() {
				server.close();
				future.complete();
			}
		});
		return future;
	}

	/**
	 * Starts this NioService, sets future true, if it was successfully, else sets exception which was throwing.
	 *
	 * @param eventloopService the NioService which will be ran.
	 */
	public static CompletionCallbackFuture startFuture(final EventloopService eventloopService) {
		final CompletionCallbackFuture future = new CompletionCallbackFuture();
		eventloopService.getEventloop().execute(new Runnable() {
			@Override
			public void run() {
				eventloopService.start(future);
			}
		});
		return future;
	}

	/**
	 * Stops this NioService, sets future true, if it was successfully, else sets exception which was throwing.
	 *
	 * @param eventloopService the NioService which will be stopped.
	 */
	public static CompletionCallbackFuture stopFuture(final EventloopService eventloopService) {
		final CompletionCallbackFuture future = new CompletionCallbackFuture();
		eventloopService.getEventloop().execute(new Runnable() {
			@Override
			public void run() {
				eventloopService.stop(future);
			}
		});
		return future;
	}

	/**
	 * It represents exception which can emerge during parallel execution. It contains results which
	 * have been already received and exception which was threw during execution
	 */
	public static final class ParallelExecutionException extends Exception {
		public final Object[] results;
		public final Exception[] exceptions;

		public ParallelExecutionException(Object[] results, Exception[] exceptions) {
			this.results = results;
			this.exceptions = exceptions;
		}
	}

	/**
	 * Returns {@link ResultCallback} which forwards {@code sendResult()} or {@code fireException()} calls
	 * to specified eventloop
	 *
	 * @param eventloop {@link Eventloop} to which calls will be forwarded
	 * @param callback  {@link ResultCallback}
	 * @param <T>
	 * @return {@link ResultCallback} which forwards {@code sendResult()} or {@code fireException()} calls
	 * to specified eventloop
	 */
	public static <T> ResultCallback<T> concurrentResultCallback(final Eventloop eventloop,
	                                                             final ResultCallback<T> callback) {
		return new ResultCallback<T>() {
			@Override
			protected void onResult(final T result) {
				eventloop.execute(new Runnable() {
					@Override
					public void run() {
						callback.sendResult(result);
					}
				});
			}

			@Override
			protected void onException(final Exception exception) {
				eventloop.execute(new Runnable() {
					@Override
					public void run() {
						callback.fireException(exception);
					}
				});
			}
		};
	}

	/**
	 * Returns {@link CompletionCallback} which forwards {@code complete()} or {@code fireException()} calls
	 * to specified eventloop
	 *
	 * @param eventloop {@link Eventloop} to which calls will be forwarded
	 * @param callback  {@link CompletionCallback}
	 * @return {@link CompletionCallback} which forwards {@code complete()} or {@code fireException()} calls
	 * to specified eventloop
	 */
	public static CompletionCallback concurrentCompletionCallback(final Eventloop eventloop,
	                                                              final CompletionCallback callback) {
		return new CompletionCallback() {
			@Override
			protected void onComplete() {
				eventloop.execute(new Runnable() {
					@Override
					public void run() {
						callback.complete();
					}
				});
			}

			@Override
			protected void onException(final Exception exception) {
				eventloop.execute(new Runnable() {
					@Override
					public void run() {
						callback.fireException(exception);
					}
				});
			}
		};
	}
}

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

package io.datakernel.rpc.client.sender;

import io.datakernel.async.ResultCallback;
import io.datakernel.rpc.client.RpcClientConnection;
import io.datakernel.rpc.client.RpcClientConnectionPool;
import io.datakernel.rpc.protocol.RpcMessage;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.OpenDataException;
import java.net.InetSocketAddress;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

final class RequestSenderToAll implements RequestSender {
	private static final RpcNoConnectionsException NO_AVAILABLE_CONNECTION = new RpcNoConnectionsException();
	private final RpcClientConnectionPool connections;

	public RequestSenderToAll(RpcClientConnectionPool connections) {
		this.connections = checkNotNull(connections);
	}

	@Override
	public <T extends RpcMessage.RpcMessageData> void sendRequest(RpcMessage.RpcMessageData request, int timeout, final ResultCallback<T> callback) {
		checkNotNull(callback);
		int calls = 0;
		FirstResultCallback<T> resultCallback = new FirstResultCallback<>(callback);
		for (InetSocketAddress address : connections.addresses()) {
			RpcClientConnection connection = connections.get(address);
			if (connection == null) {
				continue;
			}
			calls++;
			connection.callMethod(request, timeout, resultCallback);
		}
		if (calls == 0) {
			callback.onException(NO_AVAILABLE_CONNECTION);
		} else {
			resultCallback.resultOf(calls);
		}
	}

	@Override
	public void onConnectionsUpdated() {
	}

	// JMX
	@Override
	public void resetStats() {
	}

	@Override
	public CompositeData getRequestSenderInfo() throws OpenDataException {
		return null;
	}


	private static final class FirstResultCallback<T> implements ResultCallback<T> {
		private final ResultCallback<T> resultCallback;
		private T result;
		private Exception exception;
		private int countCalls;
		private int awaitsCalls;
		private boolean hasResult;
		private boolean complete;

		public FirstResultCallback(ResultCallback<T> resultCallback) {
			checkNotNull(resultCallback);
			this.resultCallback = resultCallback;
		}

		@Override
		public final void onResult(T result) {
			++countCalls;
			if (!hasResult && isValidResult(result)) {
				this.result = result;  // first valid result
				this.hasResult = true;
			}
			processResult();
		}

		protected boolean isValidResult(T result) {
			return result != null;
		}

		@Override
		public final void onException(Exception exception) {
			++countCalls;
			if (!hasResult) {
				this.exception = exception; // last Exception
			}
		}

		public void resultOf(int maxAwaitsCalls) {
			checkArgument(maxAwaitsCalls > 0);
			this.awaitsCalls = maxAwaitsCalls;
			processResult();
		}

		private boolean resultReady() {
			return awaitsCalls > 0 && (countCalls == awaitsCalls || hasResult);
		}

		private void processResult() {
			if (complete || !resultReady()) return;
			complete = true;
			if (hasResult || exception == null)
				resultCallback.onResult(result);
			else
				resultCallback.onException(exception);
		}
	}

}

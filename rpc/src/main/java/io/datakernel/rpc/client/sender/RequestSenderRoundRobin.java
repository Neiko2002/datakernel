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
import io.datakernel.jmx.CompositeDataBuilder;
import io.datakernel.rpc.client.RpcClientConnection;
import io.datakernel.rpc.client.RpcClientConnectionPool;
import io.datakernel.rpc.protocol.RpcMessage.RpcMessageData;

import javax.management.openmbean.ArrayType;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.SimpleType;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

final class RequestSenderRoundRobin implements RequestSender {
	private static final RpcNoConnectionsException NO_AVAILABLE_CONNECTION = new RpcNoConnectionsException();
	private final RpcClientConnectionPool connections;
	private final List<RequestSender> subSenders;
	private int activeConnection = 0;
	private int activeSubSender = 0;

	// JMX
	private final long[] callCounters;

	public RequestSenderRoundRobin(RpcClientConnectionPool connections) {
		this.connections = checkNotNull(connections);
		this.subSenders = null;

		this.callCounters = new long[connections.addresses().size()];
	}

	public RequestSenderRoundRobin(List<RequestSender> requestSenders) {
		this.subSenders = checkNotNull(requestSenders);
		this.connections = null;

		this.callCounters = null;
	}

	@Override
	public <T extends RpcMessageData> void sendRequest(RpcMessageData request, int timeout, ResultCallback<T> callback) {
		checkNotNull(callback);
		if (connections != null) {
			while (connections.size() > 0) {
				for (int i = 0; i < connections.addresses().size(); ++i) {
					int serverNumber = getServerNumber();
					InetSocketAddress address = connections.addresses().get(serverNumber);
					RpcClientConnection connection = connections.get(address);
					if (connection == null) {
						continue;
					}
					++callCounters[serverNumber];
					connection.callMethod(request, timeout, callback);
					return;
				}
			}
			callback.onException(NO_AVAILABLE_CONNECTION);
		} else {
			while (subSenders.size() > 0) {
				for (int i = 0; i < connections.addresses().size(); ++i) {
					int subSenderNumber = getSubSenderNumber();
					RequestSender subSender = subSenders.get(subSenderNumber);
					if (subSender == null) {
						continue;
					}
//					++callCounters[serverNumber];
					subSender.sendRequest(request, timeout, callback);
					return;
				}
			}
			callback.onException(NO_AVAILABLE_CONNECTION);
		}
	}

	private int getServerNumber() {
		activeConnection = (activeConnection + 1) % connections.addresses().size();
		return activeConnection;
	}

	private int getSubSenderNumber() {
		activeSubSender = (activeSubSender + 1) % subSenders.size();
		return activeSubSender;
	}

	@Override
	public void onConnectionsUpdated() {
	}

	// JMX
	@Override
	public void resetStats() {
		for (int i = 0; i < callCounters.length; i++) {
			callCounters[i] = 0;
		}
	}

	@Override
	public CompositeData getRequestSenderInfo() throws OpenDataException {
		List<String> res = new ArrayList<>();
		res.add("address;calls");
		for (int i = 0; i < connections.addresses().size(); i++) {
			res.add(connections.addresses().get(i) + ";" + callCounters[i]);
		}
		return CompositeDataBuilder.builder(RequestSenderRoundRobin.class.getSimpleName())
				.add("connectionDispatcher", SimpleType.STRING, RequestSenderRoundRobin.class.getSimpleName())
				.add("callsPerAddress", new ArrayType<>(1, SimpleType.STRING), res.toArray(new String[res.size()]))
				.build();
	}
}

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

package io.datakernel.dns;

import io.datakernel.async.ResultCallback;

import java.net.InetAddress;

/**
 * Resolves the IP address for the specified host name, or null if the given host is not recognized or
 * the associated IP address cannot be used to build an InetAddress instance.
 */
public interface DnsClient {
	/**
	 * Resolves a IP for the IPv4 addresses and handles it with callback
	 *
	 * @param domainName domain name for searching IP
	 * @param callback   result callback
	 */
	void resolve4(String domainName, ResultCallback<InetAddress[]> callback);

	/**
	 * Resolves a IP for the IPv6 addresses and handles it with callback
	 *
	 * @param domainName domain name for searching IP
	 * @param callback   result callback
	 */
	void resolve6(String domainName, ResultCallback<InetAddress[]> callback);
}



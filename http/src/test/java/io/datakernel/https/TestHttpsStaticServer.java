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

package io.datakernel.https;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.http.SslUtils;
import io.datakernel.http.StaticServletForResources;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TestHttpsStaticServer {
	public static final int PORT = 5568;

	static {
		Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
		root.setLevel(Level.TRACE);
	}

	public static void main(String[] args) throws Exception {
//		System.setProperty("javax.net.debug", "all");

		Eventloop eventloop = new Eventloop();
		ExecutorService executor = Executors.newCachedThreadPool();
		final AsyncHttpServer server = new AsyncHttpServer(eventloop, StaticServletForResources.create(eventloop, executor, "static/"));

		server.enableSsl(SslUtils.createSslContext("TLSv1",
				SslUtils.createKeyManagers("./src/test/resources/keystore.jks", "testtest", "testtest"),
				SslUtils.createTrustManagers("./src/test/resources/truststore.jks", "testtest"),
				new SecureRandom()), Executors.newCachedThreadPool());

		server.setListenPort(PORT);
		server.listen();

		System.out.println("https://127.0.0.1:" + PORT);
		eventloop.run();
	}
}

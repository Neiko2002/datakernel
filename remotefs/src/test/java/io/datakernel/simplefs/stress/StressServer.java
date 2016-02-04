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

package io.datakernel.simplefs.stress;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.simplefs.SimpleFsServer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;

import static io.datakernel.async.AsyncCallbacks.ignoreCompletionCallback;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class StressServer {
	public static final Path STORAGE_PATH = Paths.get("./test_data/server_storage");
	public static final Path TMP_PATH = Paths.get("./test_data/tmp");
	public static final int PORT = 5560;

	private static final ExecutorService executor = newCachedThreadPool();
	private static final Eventloop eventloop = new Eventloop();

	public static EventloopService server = SimpleFsServer.build(eventloop, executor, STORAGE_PATH, TMP_PATH)
			.setListenPort(PORT)
			.build();

	public static void main(String[] args) throws IOException {
		Files.createDirectories(STORAGE_PATH);
		server.start(ignoreCompletionCallback());
		eventloop.run();
	}
}

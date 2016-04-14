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

package io.datakernel;

import com.google.common.base.Charsets;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ForwardingResultCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.file.AsyncFile;
import io.datakernel.stream.file.StreamFileReader;
import io.datakernel.stream.file.StreamFileWriter;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static io.datakernel.async.AsyncCallbacks.ignoreCompletionCallback;
import static io.datakernel.bytebuf.ByteBufPool.getPoolItemsString;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.junit.Assert.*;

public class TestFileSystem {
	@Rule
	public final TemporaryFolder tmpFolder = new TemporaryFolder();

	private Eventloop eventloop = new Eventloop();
	private ExecutorService executor = newCachedThreadPool();
	private Path storage;
	private Path client;

	private static final int bufferSize = 16 * 1024 * 1024;

	@Before
	public void setup() throws IOException {
		storage = Paths.get(tmpFolder.newFolder("storage").toURI());
		client = Paths.get(tmpFolder.newFolder("client").toURI());

		Files.createDirectories(storage);
		Files.createDirectories(client);

		Path f = client.resolve("f.txt");
		Files.write(f, ("some text1\n\nmore text1\t\n\n\r").getBytes(Charsets.UTF_8));

		Path c = client.resolve("c.txt");
		Files.write(c, ("some text2\n\nmore text2\t\n\n\r").getBytes(Charsets.UTF_8));

		Files.createDirectories(storage.resolve("1"));
		Files.createDirectories(storage.resolve("2/3"));
		Files.createDirectories(storage.resolve("2/b"));

		Path a1 = storage.resolve("1/a.txt");
		Files.write(a1, ("1\n2\n3\n4\n5\n6\n").getBytes(Charsets.UTF_8));

		Path b = storage.resolve("1/b.txt");
		Files.write(b, ("7\n8\n9\n10\n11\n12\n").getBytes(Charsets.UTF_8));

		Path a2 = storage.resolve("2/3/a.txt");
		Files.write(a2, ("6\n5\n4\n3\n2\n1\n").getBytes(Charsets.UTF_8));

		Path d = storage.resolve("2/b/d.txt");
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < 1_000_000; i++) {
			sb.append(i).append("\n");
		}
		Files.write(d, sb.toString().getBytes(Charsets.UTF_8));

		Path e = storage.resolve("2/b/e.txt");
		Files.createFile(e);
	}

	@Test
	public void testSave() throws IOException {
		final FileSystem fs = FileSystem.newInstance(eventloop, executor, storage);
		fs.initDirectories();

		AsyncFile.open(eventloop, executor, client.resolve("c.txt"), new OpenOption[]{StandardOpenOption.READ}, new ResultCallback<AsyncFile>() {
			@Override
			protected void onResult(final AsyncFile file) {
				fs.save("1/c.txt", new ForwardingResultCallback<AsyncFile>(this) {
					@Override
					protected void onResult(AsyncFile result) {
						StreamFileReader producer = StreamFileReader.readFileFully(eventloop, file, bufferSize);
						StreamFileWriter consumer = StreamFileWriter.create(eventloop, result);
						producer.streamTo(consumer);
					}
				});
			}

			@Override
			protected void onException(Exception ignored) {
			}
		});

		eventloop.run();

		eventloop.run();
		executor.shutdown();

		assertFalse(Files.exists(storage.resolve("tmp/1/c.txt.partial")));
		assertTrue(com.google.common.io.Files.equal(client.resolve("c.txt").toFile(), storage.resolve("1/c.txt").toFile()));
		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testGet() throws IOException {
		final FileSystem fs = FileSystem.newInstance(eventloop, executor, storage);

		fs.initDirectories();

		AsyncFile.open(eventloop, executor, client.resolve("d.txt"), new OpenOption[]{CREATE_NEW, WRITE}, new ResultCallback<AsyncFile>() {
			@Override
			protected void onResult(final AsyncFile file) {
				fs.get("2/b/d.txt", new ForwardingResultCallback<AsyncFile>(this) {
					@Override
					protected void onResult(AsyncFile result) {
						StreamFileReader producer = StreamFileReader.readFileFully(eventloop, result, bufferSize);
						StreamFileWriter consumer = StreamFileWriter.create(eventloop, file);
						producer.streamTo(consumer);
					}
				});
			}

			@Override
			protected void onException(Exception ignored) {
			}
		});

		eventloop.run();
		executor.shutdown();
		assertTrue(com.google.common.io.Files.equal(client.resolve("d.txt").toFile(), storage.resolve("2/b/d.txt").toFile()));
		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testGetFailed() throws Exception {
		final FileSystem fs = FileSystem.newInstance(eventloop, executor, storage);

		fs.initDirectories();

		AsyncFile.open(eventloop, executor, client.resolve("to_be_deleted.txt"), new OpenOption[]{CREATE_NEW, WRITE}, new ResultCallback<AsyncFile>() {
			@Override
			protected void onResult(final AsyncFile file) {
				fs.get("no_file.txt", new ResultCallback<AsyncFile>() {
					@Override
					protected void onResult(AsyncFile result) {
						StreamFileReader producer = StreamFileReader.readFileFully(eventloop, result, bufferSize);
						StreamFileWriter consumer = StreamFileWriter.create(eventloop, file);
						producer.streamTo(consumer);
					}

					@Override
					protected void onException(Exception ignored) {
						AsyncFile.delete(eventloop, executor, client.resolve("to_be_deleted.txt"), ignoreCompletionCallback());
					}
				});
			}

			@Override
			protected void onException(Exception ignored) {
				AsyncFile.delete(eventloop, executor, client.resolve("to_be_deleted.txt"), ignoreCompletionCallback());
			}
		});

		eventloop.run();
		executor.shutdown();

		assertFalse(Files.exists(client.resolve("to_be_deleted.txt")));
		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testDeleteFile() throws IOException {
		FileSystem fs = FileSystem.newInstance(eventloop, executor, storage);

		fs.initDirectories();
		assertTrue(Files.exists(storage.resolve("2/3/a.txt")));
		fs.delete("2/3/a.txt", ignoreCompletionCallback());
		eventloop.run();
		assertFalse(Files.exists(storage.resolve("2/3/a.txt")));
		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testDeleteFailed() throws IOException {
		FileSystem fs = FileSystem.newInstance(eventloop, executor, storage);

		fs.initDirectories();
		fs.delete("2/3/z.txt", new CompletionCallback() {
			@Override
			protected void onComplete() {
				fail("Should not end here");
			}

			@Override
			protected void onException(Exception e) {
				assertTrue(e.getClass() == NoSuchFileException.class);
			}
		});
		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testListFiles() throws IOException {
		FileSystem fs = FileSystem.newInstance(eventloop, executor, storage);

		fs.initDirectories();
		final List<String> expected = new ArrayList<>();
		expected.addAll(Arrays.asList("1/a.txt", "1/b.txt", "2/3/a.txt", "2/b/d.txt", "2/b/e.txt"));
		fs.list(new ResultCallback<List<String>>() {
			@Override
			protected void onResult(List<String> result) {
				Collections.sort(result);
				Collections.sort(expected);
				assertEquals(expected, result);
			}

			@Override
			protected void onException(Exception exception) {
				fail("Should not get here");
			}
		});

		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}
}

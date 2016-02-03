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

import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.file.StreamFileReader;
import io.datakernel.stream.file.StreamFileWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static io.datakernel.util.Preconditions.check;
import static io.datakernel.util.Preconditions.checkNotNull;

public final class FileSystem {
	public static final String DEFAULT_IN_PROGRESS_EXTENSION = ".partial";
	public static final int DEFAULT_READER_BUFFER_SIZE = 256 * 1024;

	private static final Logger logger = LoggerFactory.getLogger(FileSystem.class);
	private final String inProgressExtension;
	private final int bufferSize;

	private final Eventloop eventloop;
	private final ExecutorService executor;

	private final Path fileStorage;
	private final Path tmpStorage;

	private FileSystem(Eventloop eventloop, ExecutorService executor,
	                   Path fileStorage, Path tmpStorage, int bufferSize,
	                   String inProgressExtension) {

		this.eventloop = checkNotNull(eventloop);
		this.executor = checkNotNull(executor);

		check(!(fileStorage.startsWith(tmpStorage) || tmpStorage.startsWith(fileStorage)), "Directories must not relate(i.e. parent-child)");
		this.fileStorage = fileStorage;
		this.tmpStorage = tmpStorage;

		check(bufferSize > 0, "BufferSize should be positive %s", bufferSize);
		this.bufferSize = bufferSize;

		this.inProgressExtension = checkNotNull(inProgressExtension);
	}

	public static FileSystem newInstance(Eventloop eventloop, ExecutorService executor, Path storage, Path tmpStorage) {
		return new FileSystem(eventloop, executor, storage, tmpStorage, DEFAULT_READER_BUFFER_SIZE, DEFAULT_IN_PROGRESS_EXTENSION);
	}

	public static FileSystem newInstance(Eventloop eventloop, ExecutorService executor,
	                                     Path storage, Path tmpStorage, int bufferSize,
	                                     String inProgressExtension) {
		return new FileSystem(eventloop, executor, storage, tmpStorage, bufferSize, inProgressExtension);
	}

	public void saveToTmp(String fileName, StreamProducer<ByteBuf> producer, CompletionCallback callback) {
		logger.trace("Saving to temporary dir {}", fileName);
		Path tmpPath;
		try {
			tmpPath = ensureInProgressDirectory(fileName);
			StreamFileWriter diskWrite = StreamFileWriter.createFile(eventloop, executor, tmpPath, true, true);
			producer.streamTo(diskWrite);
			diskWrite.setFlushCallback(callback);
		} catch (IOException e) {
			logger.trace("Caught exception while trying to ensure in-progress-directory: {}", e.getMessage());
			callback.onException(e);
		}
	}

	public void commitTmp(String fileName, CompletionCallback callback) {
		logger.trace("Moving file from temporary dir to {}", fileName);
		Path destinationPath;
		Path tmpPath;
		try {
			tmpPath = ensureInProgressDirectory(fileName);
			destinationPath = ensureDestinationDirectory(fileName);
		} catch (IOException e) {
			logger.error("Can't ensure directory for {}", fileName, e);
			callback.onException(e);
			return;
		}
		try {
			Files.move(tmpPath, destinationPath);
			callback.onComplete();
		} catch (IOException e) {
			logger.error("Can't move file from temporary dir to {}", fileName, e);
			try {
				Files.delete(tmpPath);
			} catch (IOException ignored) {
				logger.error("Can't delete file that didn't manage to move from temporary", ignored);
			}
			callback.onException(e);
		}
	}

	public void deleteTmp(String fileName, CompletionCallback callback) {
		logger.trace("Deleting temporary file {}", fileName);
		Path path = tmpStorage.resolve(fileName + inProgressExtension);
		try {
			Files.delete(path);
			callback.onComplete();
		} catch (IOException e) {
			logger.error("Can't delete temporary file {}", fileName, e);
			callback.onException(e);
		}
	}

	public StreamProducer<ByteBuf> get(String fileName, long startPosition) {
		logger.trace("Streaming file {}", fileName);
		Path destination = fileStorage.resolve(fileName);
		return StreamFileReader.readFileFrom(eventloop, executor, bufferSize, destination, startPosition);
	}

	public void delete(String fileName, CompletionCallback callback) {
		logger.trace("Deleting file {}", fileName);
		Path path = fileStorage.resolve(fileName);
		try {
			Files.delete(path);
			callback.onComplete();
		} catch (IOException e) {
			logger.error("Can't delete file {}", fileName, e);
			callback.onException(e);
		}
	}

	public void list(ResultCallback<Set<String>> callback) {
		logger.trace("Listing files");
		Set<String> result = new HashSet<>();
		try {
			listFiles(fileStorage, result, "");
			callback.onResult(result);
		} catch (IOException e) {
			logger.error("Can't list files", e);
			callback.onException(e);
		}
	}

	public long fileSize(String filePath) {
		File file = fileStorage.resolve(filePath).toFile();
		if (!file.exists() || file.isDirectory()) {
			return -1;
		} else {
			return file.length();
		}
	}

	private void listFiles(Path parent, Set<String> files, String previousPath) throws IOException {
		try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(parent)) {
			for (Path path : directoryStream) {
				if (Files.isDirectory(path)) {
					if (!path.equals(tmpStorage)) {
						listFiles(path, files, previousPath + path.getFileName().toString() + File.separator);
					}
				} else {
					files.add(previousPath + path.getFileName().toString());
				}
			}
		}
	}

	private Path ensureDestinationDirectory(String path) throws IOException {
		return ensureDirectory(fileStorage, path);
	}

	private Path ensureInProgressDirectory(String path) throws IOException {
		return ensureDirectory(tmpStorage, path + inProgressExtension);
	}

	private Path ensureDirectory(Path container, String path) throws IOException {
		String fileName = getFileName(path);
		String filePath = getFilePath(path);

		Path destinationDirectory = container.resolve(filePath);

		Files.createDirectories(destinationDirectory);

		return destinationDirectory.resolve(fileName);
	}

	private String getFileName(String path) {
		if (path.contains(File.separator)) {
			path = path.substring(path.lastIndexOf(File.separator) + 1);
		}
		return path;
	}

	private String getFilePath(String path) {
		if (path.contains(File.separator)) {
			path = path.substring(0, path.lastIndexOf(File.separator));
		} else {
			path = "";
		}
		return path;
	}

	public void initDirectories() throws IOException {
		Files.createDirectories(fileStorage);
		Files.createDirectories(tmpStorage);
		cleanDirectory(tmpStorage);
	}

	private void cleanDirectory(Path directory) throws IOException {
		try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(directory)) {
			for (Path path : directoryStream) {
				if (Files.isDirectory(path) && path.iterator().hasNext()) {
					cleanDirectory(path);
				}
				Files.delete(path);
			}
		}
	}
}
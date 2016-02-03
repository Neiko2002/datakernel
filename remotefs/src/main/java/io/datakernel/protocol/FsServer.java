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

package io.datakernel.protocol;

import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.stream.StreamProducer;

import java.util.Set;

public abstract class FsServer {
	protected abstract void upload(String fileName, StreamProducer<ByteBuf> producer, CompletionCallback callback);

	protected abstract void commit(String fileName, boolean success, CompletionCallback callback);

	protected StreamProducer<ByteBuf> download(String fileName) {
		return download(fileName, 0);
	}

	protected void download(String fileName, ResultCallback<StreamProducer<ByteBuf>> callback) {
		callback.onResult(download(fileName));
	}

	protected abstract StreamProducer<ByteBuf> download(String fileName, long startPosition);

	protected abstract void delete(String fileName, CompletionCallback callback);

	protected abstract void list(ResultCallback<Set<String>> callback);

	protected abstract long fileSize(String fileName);
}
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

package io.datakernel.stream.net;

import io.datakernel.async.CompletionCallback;

public interface Messaging<I, O> extends SocketStreaming {
	interface ReadCallback<I> {
		void onRead(I msg);

		void onReadEndOfStream();

		void onException(Exception e);
	}

	void read(ReadCallback<I> callback);

	void write(O msg, CompletionCallback callback);

	void writeEndOfStream(CompletionCallback callback);

	void close();
}

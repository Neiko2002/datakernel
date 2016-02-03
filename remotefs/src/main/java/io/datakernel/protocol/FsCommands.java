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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.datakernel.serializer.GsonSubclassesAdapter;

public abstract class FsCommands {
	public static abstract class FsCommand {

	}

	public static class Upload extends FsCommand {
		public final String filePath;

		public Upload(String filePath) {
			this.filePath = filePath;
		}

		@Override
		public String toString() {
			return "Upload{filepath=\'" + filePath + "\'}";
		}
	}

	public static class Commit extends FsCommand {
		public final String filePath;

		public final boolean isOk;

		public Commit(String filePath, boolean isOk) {
			this.filePath = filePath;
			this.isOk = isOk;
		}

		@Override
		public String toString() {
			return "Commit{filepath=\'" + filePath + "\',isOk=" + isOk + "}";
		}
	}

	public static class Delete extends FsCommand {
		public final String filePath;

		public Delete(String filePath) {
			this.filePath = filePath;
		}

		@Override
		public String toString() {
			return "Delete{filepath=\'" + filePath + "\'}";
		}
	}

	public static class Download extends FsCommand {
		public final String filePath;
		public final long startPosition;

		public Download(String filePath, long startPosition) {
			this.filePath = filePath;
			this.startPosition = startPosition;
		}

		@Override
		public String toString() {
			return "Delete{filepath=\'" + filePath + "\',startPosition=" + startPosition + "}";
		}
	}

	public static class ListFiles extends FsCommand {
		@Override
		public String toString() {
			return "List{all files}";
		}
	}

	public static Gson commandGSON = new GsonBuilder()
			.registerTypeAdapter(FsCommand.class, GsonSubclassesAdapter.builder()
					.subclassField("commandType")
					.subclass("Upload", Upload.class)
					.subclass("Commit", Commit.class)
					.subclass("Download", Download.class)
					.subclass("Delete", Delete.class)
					.subclass("List", ListFiles.class)
					.build())
			.setPrettyPrinting()
			.enableComplexMapKeySerialization()
			.create();
}
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

package io.datakernel.hashfs;

import io.datakernel.async.ResultCallback;

import java.util.List;
import java.util.Set;

interface Commands {
	void scan(ResultCallback<List<String>> callback);

	void updateServerMap(Set<ServerInfo> bootstrap);

	void delete(String fileName);

	void replicate(ServerInfo server, String fileName);

	void offer(ServerInfo server, List<String> forUpload, List<String> forDeletion, ResultCallback<List<String>> callback);

	void scheduleUpdate();

	void scheduleCommitCancel(String fileName, long waitTime);
}
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

package io.datakernel.async;

/**
 * This callback's interface contains two methods onResult and onException.
 * The async operation must call either onResult or onException when it is complete.
 *
 * @param <T> type of result
 */
public interface ResultCallback<T> extends ExceptionCallback {
	/**
	 * Handles result of async operation
	 *
	 * @param result result of async operation
	 */
	void onResult(T result);
}

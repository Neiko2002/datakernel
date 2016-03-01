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

package io.datakernel.jmx;

import javax.management.openmbean.OpenType;
import java.util.List;
import java.util.Map;

interface AttributeNode {
	String getName();

	OpenType<?> getOpenType();

	Map<String, OpenType<?>> getFlattenedOpenTypes();

	Map<String, Object> aggregateAllAttributes(List<?> pojos);

	Object aggregateAttribute(List<?> pojos, String attrName);

	void refresh(List<?> pojos, long timestamp, double smoothingWindow);

	boolean isRefreshable();

//	boolean isSettable();
//
//	void setAttribute(Object value);
}
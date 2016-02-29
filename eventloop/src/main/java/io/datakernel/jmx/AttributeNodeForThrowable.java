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

import javax.management.openmbean.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.datakernel.jmx.OpenTypeUtils.createMapWithOneEntry;
import static io.datakernel.util.Preconditions.checkArgument;
import static java.util.Arrays.asList;

final class AttributeNodeForThrowable implements AttributeNode {
	private static final String THROWABLE_TYPE_KEY = "type";
	private static final String THROWABLE_MESSAGE_KEY = "message";
	private static final String THROWABLE_STACK_TRACE_KEY = "stackTrace";

	private final String name;
	private final ValueFetcher fetcher;
	private final CompositeType compositeType;
	private final Map<String, OpenType<?>> nameToOpenType;

	public AttributeNodeForThrowable(String name, ValueFetcher fetcher) {
		this.name = name;
		this.fetcher = fetcher;
		this.compositeType = compositeTypeForThrowable();
		this.nameToOpenType = createMapWithOneEntry(name, compositeType);
	}

	private static CompositeType compositeTypeForThrowable() {
		try {
			String[] itemNames = new String[]{THROWABLE_TYPE_KEY, THROWABLE_MESSAGE_KEY, THROWABLE_STACK_TRACE_KEY};
			OpenType<?>[] itemTypes = new OpenType<?>[]{
					SimpleType.STRING, SimpleType.STRING, new ArrayType<>(1, SimpleType.STRING)};
			return new CompositeType("CompositeType", "CompositeType", itemNames, itemNames, itemTypes);
		} catch (OpenDataException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public OpenType<?> getOpenType() {
		return compositeType;
	}

	@Override
	public Map<String, OpenType<?>> getFlattenedOpenTypes() {
		return nameToOpenType;
	}

	@Override
	public Map<String, Object> aggregateAllAttributes(List<?> pojos) {
		Map<String, Object> attrs = new HashMap<>();
		attrs.put(name, aggregateAttribute(pojos, null));
		return attrs;
	}

	@Override
	public Object aggregateAttribute(List<?> pojos, String attrName) {
		// TODO(vmykhalko): is this check needed ?
//		checkPojos(pojos);
		checkArgument(attrName == null || attrName.isEmpty());

		// we ignore attrName here because this is leaf-node
		Object firstPojo = pojos.get(0);
		Throwable firstThrowable = (Throwable) fetcher.fetchFrom(firstPojo);
		Throwable resultThrowable = firstThrowable;
		for (int i = 1; i < pojos.size(); i++) {
			Object currentPojo = pojos.get(i);
			Throwable currentThrowable = (Throwable) fetcher.fetchFrom(currentPojo);
			if (currentThrowable != null) {
				resultThrowable = currentThrowable;
			}
		}

		try {
			return createCompositeDataFor(resultThrowable);
		} catch (OpenDataException e) {
			throw new RuntimeException(e);
		}
	}

	private CompositeData createCompositeDataFor(Throwable throwable) throws OpenDataException {
		CompositeData compositeData = null;
		if (throwable != null) {
			String type = throwable.getClass().getName();
			String msg = throwable.getMessage();
			List<String> stackTrace = asList(MBeanFormat.formatException(throwable));
			Map<String, Object> nameToValue = new HashMap<>();
			nameToValue.put(THROWABLE_TYPE_KEY, type);
			nameToValue.put(THROWABLE_MESSAGE_KEY, msg);
			nameToValue.put(THROWABLE_STACK_TRACE_KEY, stackTrace.toArray(new String[stackTrace.size()]));
			compositeData = new CompositeDataSupport(compositeType, nameToValue);
		}
		return compositeData;
	}

	@Override
	public void refresh(List<?> pojos, long timestamp, double smoothingWindow) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isRefreshable() {
		return false;
	}
}
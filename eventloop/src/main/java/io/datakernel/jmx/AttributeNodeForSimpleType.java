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
import javax.management.openmbean.SimpleType;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static io.datakernel.jmx.Utils.createMapWithOneEntry;
import static io.datakernel.jmx.Utils.filterNulls;
import static io.datakernel.util.Preconditions.checkArgument;
import static io.datakernel.util.Preconditions.checkNotNull;

final class AttributeNodeForSimpleType implements AttributeNode {
	private final String name;
	private final ValueFetcher fetcher;
	private final Method setter;
	private final OpenType<?> openType;
	private final Map<String, OpenType<?>> nameToOpenType;

	public AttributeNodeForSimpleType(String name, ValueFetcher fetcher, Method setter, Class<?> attributeType) {
		checkArgument(!name.isEmpty(), "SimpleType attribute cannot have empty name");

		this.name = name;
		this.fetcher = fetcher;
		this.setter = setter;
		this.openType = simpleTypeOf(attributeType);
		this.nameToOpenType = createMapWithOneEntry(name, openType);
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public OpenType<?> getOpenType() {
		return openType;
	}

	@Override
	public Map<String, OpenType<?>> getFlattenedOpenTypes() {
		return nameToOpenType;
	}

	@Override
	public Map<String, Object> aggregateAllAttributes(List<?> sources) {
		Map<String, Object> attrs = new HashMap<>();
		attrs.put(name, aggregateAttribute(name, sources));
		return attrs;
	}

	@Override
	public Object aggregateAttribute(String attrName, List<?> sources) {
		checkArgument(attrName.equals(name));
		checkNotNull(sources);
		List<?> notNullSources = filterNulls(sources);
		if (notNullSources.size() == 0) {
			return null;
		}

		Object firstPojo = notNullSources.get(0);
		Object firstValue = fetcher.fetchFrom(firstPojo);
		for (int i = 1; i < notNullSources.size(); i++) {
			Object currentPojo = notNullSources.get(i);
			Object currentValue = fetcher.fetchFrom(currentPojo);
			if (!Objects.equals(firstValue, currentValue)) {
				return null;
			}
		}
		return firstValue;
	}

	@Override
	public void refresh(List<?> targets, long timestamp, double smoothingWindow) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isRefreshable() {
		return false;
	}

	@Override
	public boolean isSettable(String attrName) {
		checkArgument(attrName.equals(name));

		return setter != null;
	}

	@Override
	public void setAttribute(String attrName, Object value, List<?> targets) {
		checkArgument(attrName.equals(name));
		checkNotNull(targets);
		List<?> notNullTargets = filterNulls(targets);
		if (notNullTargets.size() == 0) {
			return;
		}

		for (Object target : notNullTargets) {
			try {
				setter.invoke(target, value);
			} catch (IllegalAccessException | InvocationTargetException e) {
				throw new RuntimeException(e);
			}
		}
	}

	private static SimpleType<?> simpleTypeOf(Class<?> clazz) throws IllegalArgumentException {
		if (clazz == boolean.class || clazz == Boolean.class) {
			return SimpleType.BOOLEAN;
		} else if (clazz == byte.class || clazz == Byte.class) {
			return SimpleType.BYTE;
		} else if (clazz == short.class || clazz == Short.class) {
			return SimpleType.SHORT;
		} else if (clazz == char.class || clazz == Character.class) {
			return SimpleType.CHARACTER;
		} else if (clazz == int.class || clazz == Integer.class) {
			return SimpleType.INTEGER;
		} else if (clazz == long.class || clazz == Long.class) {
			return SimpleType.LONG;
		} else if (clazz == float.class || clazz == Float.class) {
			return SimpleType.FLOAT;
		} else if (clazz == double.class || clazz == Double.class) {
			return SimpleType.DOUBLE;
		} else if (clazz == String.class) {
			return SimpleType.STRING;
		} else {
			throw new IllegalArgumentException("There is no SimpleType for " + clazz.getName());
		}
	}
}

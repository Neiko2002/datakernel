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

package io.datakernel.aggregation_db.keytype;

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import io.datakernel.serializer.asm.SerializerGen;

import java.util.Comparator;

/**
 * Represents a type of aggregation key. It can be enumerable (integer, for example) or not (string or floating-point number).
 */
public abstract class KeyType implements Comparator<Object> {
	protected final Class<?> dataType;

	public KeyType(Class<?> dataType) {
		this.dataType = dataType;
	}

	public abstract SerializerGen serializerGen();

	public Class<?> getDataType() {
		return dataType;
	}

	public abstract JsonPrimitive toJson(Object value);

	public abstract Object fromJson(JsonElement value);

	@Override
	public String toString() {
		return "{" + dataType + '}';
	}
}


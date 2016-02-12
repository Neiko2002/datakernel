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

package io.datakernel.aggregation_db.gson;

import com.google.gson.*;
import io.datakernel.aggregation_db.AggregationQuery;
import io.datakernel.aggregation_db.api.QueryException;

import java.lang.reflect.Type;

public final class QueryOrderingGsonSerializer implements JsonSerializer<AggregationQuery.Ordering>,
		JsonDeserializer<AggregationQuery.Ordering> {
	@Override
	public AggregationQuery.Ordering deserialize(JsonElement json, Type type, JsonDeserializationContext ctx)
			throws JsonParseException {
		if (!(json instanceof JsonObject))
			throw new QueryException("Incorrect sort format. Should be represented as a JSON object");

		JsonObject orderingJson = (JsonObject) json;

		String orderingField = orderingJson.get("field").getAsString();
		String direction = orderingJson.get("direction").getAsString();

		if (direction.equals("asc"))
			return AggregationQuery.Ordering.asc(orderingField);

		if (direction.equals("desc"))
			return AggregationQuery.Ordering.desc(orderingField);

		throw new QueryException("Unknown 'direction' property value in sort object. Should be either 'asc' or 'desc'");
	}

	@Override
	public JsonElement serialize(AggregationQuery.Ordering ordering, Type type, JsonSerializationContext ctx) {
		JsonObject orderingJson = new JsonObject();
		orderingJson.addProperty("field", ordering.getPropertyName());
		orderingJson.addProperty("direction", ordering.isAsc() ? "asc" : "desc");
		return orderingJson;
	}
}

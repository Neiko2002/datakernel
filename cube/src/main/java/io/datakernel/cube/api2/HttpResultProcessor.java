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

package io.datakernel.cube.api2;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import io.datakernel.aggregation_db.AggregationStructure;
import io.datakernel.aggregation_db.keytype.KeyType;
import io.datakernel.codegen.utils.DefiningClassLoader;
import io.datakernel.cube.api.FieldGetter;
import io.datakernel.cube.api.TotalsPlaceholder;
import io.datakernel.http.HttpResponse;

import java.util.List;

import static io.datakernel.cube.api.CommonUtils.createResponse;
import static io.datakernel.cube.api.CommonUtils.generateGetter;

public final class HttpResultProcessor implements ResultProcessor<HttpResponse> {
	private final DefiningClassLoader classLoader;
	private final AggregationStructure structure;

	public HttpResultProcessor(DefiningClassLoader classLoader, AggregationStructure structure) {
		this.classLoader = classLoader;
		this.structure = structure;
	}

	@Override
	public HttpResponse apply(QueryResult result) {
		String response = result.isEmpty() ? constructEmptyResult() : constructResult(result.getRecords(),
				result.getRecordClass(), result.getKeys(), result.getFields(), result.getTotals());
		return createResponse(response);
	}

	private String constructEmptyResult() {
		JsonObject jsonResult = new JsonObject();
		jsonResult.add("records", new JsonArray());
		jsonResult.add("totals", new JsonObject());
		jsonResult.addProperty("count", 0);
		return jsonResult.toString();
	}

	private String constructResult(List results, Class<?> resultClass,
	                               List<String> keys, List<String> fields,
	                               TotalsPlaceholder totals) {
		JsonObject jsonResult = new JsonObject();
		JsonArray jsonRecords = new JsonArray();
		JsonObject jsonTotals = new JsonObject();

		FieldGetter[] fieldGetters = new FieldGetter[fields.size()];
		for (int i = 0; i < fields.size(); i++) {
			String field = fields.get(i);
			fieldGetters[i] = generateGetter(classLoader, resultClass, field);
		}

		FieldGetter[] keyGetters = new FieldGetter[keys.size()];
		KeyType[] keyTypes = new KeyType[keys.size()];
		for (int i = 0; i < keys.size(); i++) {
			String key = keys.get(i);
			keyGetters[i] = generateGetter(classLoader, resultClass, key);
			keyTypes[i] = structure.getKeyType(key);
		}

		for (Object result : results) {
			JsonObject resultJsonObject = new JsonObject();

			for (int j = 0; j < keys.size(); j++) {
				Object value = keyGetters[j].get(result);
				JsonElement json;
				if (keyTypes[j] == null)
					json = value == null ? null : new JsonPrimitive(value.toString());
				else
					json = keyTypes[j].toJson(value);
				resultJsonObject.add(keys.get(j), json);
			}

			for (int j = 0; j < fields.size(); j++) {
				resultJsonObject.add(fields.get(j), new JsonPrimitive((Number) fieldGetters[j].get(result)));
			}

			jsonRecords.add(resultJsonObject);
		}

		for (String field : fields) {
			Object totalFieldValue = generateGetter(classLoader, totals.getClass(), field).get(totals);
			jsonTotals.add(field, new JsonPrimitive((Number) totalFieldValue));
		}

		jsonResult.add("records", jsonRecords);
		jsonResult.add("totals", jsonTotals);
		jsonResult.addProperty("count", results.size());

		return jsonResult.toString();
	}
}

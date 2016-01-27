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
				result.getRecordClass(), result.getDimensions(), result.getAttributes(), result.getMeasures(),
				result.getTotals());
		return createResponse(response);
	}

	private String constructEmptyResult() {
		JsonObject jsonResult = new JsonObject();
		jsonResult.add("records", new JsonArray());
		jsonResult.add("totals", new JsonObject());
		jsonResult.add("metadata", new JsonObject());
		jsonResult.addProperty("count", 0);
		return jsonResult.toString();
	}

	private String constructResult(List results, Class<?> resultClass, List<String> dimensions,
	                               List<String> attributes, List<String> measures, TotalsPlaceholder totals) {
		JsonObject jsonMetadata = new JsonObject();

		JsonArray jsonMeasures = new JsonArray();
		FieldGetter[] measureGetters = new FieldGetter[measures.size()];
		for (int i = 0; i < measures.size(); ++i) {
			String field = measures.get(i);
			jsonMeasures.add(new JsonPrimitive(field));
			measureGetters[i] = generateGetter(classLoader, resultClass, field);
		}
		jsonMetadata.add("measures", jsonMeasures);

		JsonArray jsonDimensions = new JsonArray();
		FieldGetter[] dimensionGetters = new FieldGetter[dimensions.size()];
		KeyType[] keyTypes = new KeyType[dimensions.size()];
		for (int i = 0; i < dimensions.size(); ++i) {
			String key = dimensions.get(i);
			jsonDimensions.add(new JsonPrimitive(key));
			dimensionGetters[i] = generateGetter(classLoader, resultClass, key);
			keyTypes[i] = structure.getKeyType(key);
		}
		jsonMetadata.add("dimensions", jsonDimensions);

		JsonArray jsonAttributes = new JsonArray();
		FieldGetter[] attributeGetters = new FieldGetter[attributes.size()];
		for (int i = 0; i < attributes.size(); ++i) {
			String attribute = attributes.get(i);
			jsonAttributes.add(new JsonPrimitive(attribute));
			attributeGetters[i] = generateGetter(classLoader, resultClass, attribute);
		}
		jsonMetadata.add("attributes", jsonAttributes);

		JsonArray jsonRecords = new JsonArray();
		for (Object result : results) {
			JsonObject resultJsonObject = new JsonObject();

			for (int j = 0; j < dimensions.size(); ++j) {
				Object value = dimensionGetters[j].get(result);
				JsonElement json = keyTypes[j].toJson(value);
				resultJsonObject.add(dimensions.get(j), json);
			}

			for (int j = 0; j < attributes.size(); ++j) {
				Object value = attributeGetters[j].get(result);
				resultJsonObject.add(attributes.get(j), value == null ? null : new JsonPrimitive(value.toString()));
			}

			for (int j = 0; j < measures.size(); ++j) {
				resultJsonObject.add(measures.get(j), new JsonPrimitive((Number) measureGetters[j].get(result)));
			}

			jsonRecords.add(resultJsonObject);
		}

		JsonObject jsonTotals = new JsonObject();
		for (String field : measures) {
			Object totalFieldValue = generateGetter(classLoader, totals.getClass(), field).get(totals);
			jsonTotals.add(field, new JsonPrimitive((Number) totalFieldValue));
		}

		JsonObject jsonResult = new JsonObject();
		jsonResult.add("records", jsonRecords);
		jsonResult.add("totals", jsonTotals);
		jsonResult.add("metadata", jsonMetadata);
		jsonResult.addProperty("count", results.size());

		return jsonResult.toString();
	}
}

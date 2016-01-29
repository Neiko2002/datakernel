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
import java.util.Set;

import static io.datakernel.cube.api.CommonUtils.createResponse;
import static io.datakernel.cube.api.CommonUtils.generateGetter;
import static io.datakernel.cube.api2.HttpJsonConstants.*;

public final class HttpResultProcessor implements ResultProcessor<HttpResponse> {
	private final DefiningClassLoader classLoader;
	private final AggregationStructure structure;

	public HttpResultProcessor(DefiningClassLoader classLoader, AggregationStructure structure) {
		this.classLoader = classLoader;
		this.structure = structure;
	}

	@Override
	public HttpResponse apply(QueryResult result) {
		String response = constructResult(result.getRecords(), result.getRecordClass(), result.getTotals(),
				result.getCount(), result.getDrillDowns(), result.getDimensions(), result.getAttributes(),
				result.getMeasures(), result.getFilterAttributesPlaceholder(), result.getFilterAttributes(),
				result.getFilterAttributesClass());
		return createResponse(response);
	}

	private String constructResult(List results, Class resultClass, TotalsPlaceholder totals, int count,
	                               Set<List<String>> drillDowns, List<String> dimensions, List<String> attributes,
	                               List<String> measures, Object filterAttributesPlaceholder,
	                               List<String> filterAttributes, Class filterAttributesClass) {
		JsonObject jsonMetadata = new JsonObject();

		JsonArray jsonMeasures = new JsonArray();
		FieldGetter[] measureGetters = new FieldGetter[measures.size()];
		for (int i = 0; i < measures.size(); ++i) {
			String field = measures.get(i);
			jsonMeasures.add(new JsonPrimitive(field));
			measureGetters[i] = generateGetter(classLoader, resultClass, field);
		}
		jsonMetadata.add(MEASURES_FIELD, jsonMeasures);

		JsonArray jsonDimensions = new JsonArray();
		FieldGetter[] dimensionGetters = new FieldGetter[dimensions.size()];
		KeyType[] keyTypes = new KeyType[dimensions.size()];
		for (int i = 0; i < dimensions.size(); ++i) {
			String key = dimensions.get(i);
			jsonDimensions.add(new JsonPrimitive(key));
			dimensionGetters[i] = generateGetter(classLoader, resultClass, key);
			keyTypes[i] = structure.getKeyType(key);
		}
		jsonMetadata.add(DIMENSIONS_FIELD, jsonDimensions);

		JsonArray jsonAttributes = new JsonArray();
		FieldGetter[] attributeGetters = new FieldGetter[attributes.size()];
		for (int i = 0; i < attributes.size(); ++i) {
			String attribute = attributes.get(i);
			jsonAttributes.add(new JsonPrimitive(attribute));
			attributeGetters[i] = generateGetter(classLoader, resultClass, attribute);
		}
		jsonMetadata.add(ATTRIBUTES_FIELD, jsonAttributes);

		JsonObject jsonFilterAttributes = new JsonObject();
		for (String attribute : filterAttributes) {
			Object resolvedAttribute = generateGetter(classLoader, filterAttributesClass, attribute).get(filterAttributesPlaceholder);
			jsonFilterAttributes.add(attribute, resolvedAttribute == null ? null : new JsonPrimitive(resolvedAttribute.toString()));
		}
		jsonMetadata.add(FILTER_ATTRIBUTES_FIELD, jsonFilterAttributes);

		JsonArray jsonDrillDowns = new JsonArray();
		for (List<String> drillDown : drillDowns) {
			JsonArray jsonDrillDown = new JsonArray();
			for (String dimension : drillDown) {
				jsonDrillDown.add(new JsonPrimitive(dimension));
			}
			jsonDrillDowns.add(jsonDrillDown);
		}
		jsonMetadata.add(DRILLDOWNS_FIELD, jsonDrillDowns);

		JsonArray jsonRecords = new JsonArray();
		for (Object result : results) {
			JsonObject resultJsonObject = new JsonObject();

			for (int n = 0; n < dimensions.size(); ++n) {
				Object value = dimensionGetters[n].get(result);
				JsonElement json = new JsonPrimitive(keyTypes[n].toString(value));
				resultJsonObject.add(dimensions.get(n), json);
			}

			for (int m = 0; m < attributes.size(); ++m) {
				Object value = attributeGetters[m].get(result);
				resultJsonObject.add(attributes.get(m), value == null ? null : new JsonPrimitive(value.toString()));
			}

			for (int k = 0; k < measures.size(); ++k) {
				resultJsonObject.add(measures.get(k), new JsonPrimitive((Number) measureGetters[k].get(result)));
			}

			jsonRecords.add(resultJsonObject);
		}

		JsonObject jsonTotals = new JsonObject();
		for (String field : measures) {
			Object totalFieldValue = generateGetter(classLoader, totals.getClass(), field).get(totals);
			jsonTotals.add(field, new JsonPrimitive((Number) totalFieldValue));
		}

		JsonObject jsonResult = new JsonObject();
		jsonResult.add(RECORDS_FIELD, jsonRecords);
		jsonResult.add(TOTALS_FIELD, jsonTotals);
		jsonResult.add(METADATA_FIELD, jsonMetadata);
		jsonResult.addProperty(COUNT_FIELD, count);

		return jsonResult.toString();
	}
}

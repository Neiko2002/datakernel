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
import io.datakernel.cube.DrillDown;
import io.datakernel.cube.api.FieldGetter;
import io.datakernel.cube.api.TotalsPlaceholder;
import io.datakernel.http.HttpResponse;

import java.util.List;
import java.util.Set;

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
		String response = constructResult(result.getRecords(), result.getRecordClass(), result.getTotals(),
				result.getCount(), result.getDrillDowns(), result.getDimensions(), result.getAttributes(),
				result.getMeasures(), result.getFilterAttributesPlaceholder(), result.getFilterAttributes(),
				result.getMetadataFields());
		return createResponse(response);
	}

	private String constructResult(List results, Class resultClass, TotalsPlaceholder totals, int count,
	                               Set<DrillDown> drillDowns, List<String> dimensions, List<String> attributes,
	                               List<String> measures, Object filterAttributesPlaceholder,
	                               List<String> filterAttributes, Set<String> metadataFields) {
		FieldGetter[] dimensionGetters = new FieldGetter[dimensions.size()];
		KeyType[] keyTypes = new KeyType[dimensions.size()];
		for (int i = 0; i < dimensions.size(); ++i) {
			String key = dimensions.get(i);
			dimensionGetters[i] = generateGetter(classLoader, resultClass, key);
			keyTypes[i] = structure.getKeyType(key);
		}

		FieldGetter[] attributeGetters = new FieldGetter[attributes.size()];
		for (int i = 0; i < attributes.size(); ++i) {
			String attribute = attributes.get(i);
			attributeGetters[i] = generateGetter(classLoader, resultClass, attribute);
		}

		FieldGetter[] measureGetters = new FieldGetter[measures.size()];
		for (int i = 0; i < measures.size(); ++i) {
			String field = measures.get(i);
			measureGetters[i] = generateGetter(classLoader, resultClass, field);
		}

		JsonObject jsonMetadata = new JsonObject();

		if (metadataFields.contains("dimensions"))
			jsonMetadata.add("dimensions", getJsonArrayFromList(dimensions));

		if (metadataFields.contains("attributes"))
			jsonMetadata.add("attributes", getJsonArrayFromList(attributes));

		if (metadataFields.contains("measures"))
			jsonMetadata.add("measures", getJsonArrayFromList(measures));

		if (metadataFields.contains("filterAttributes"))
			jsonMetadata.add("filterAttributes", getFilterAttributesJson(filterAttributes, filterAttributesPlaceholder));

		if (metadataFields.contains("drillDowns"))
			jsonMetadata.add("drillDowns", getDrillDownsJson(drillDowns));

		JsonArray jsonRecords = getRecordsJson(results, dimensions, attributes, measures, dimensionGetters,
				attributeGetters, measureGetters, keyTypes);

		JsonObject jsonResult = new JsonObject();
		jsonResult.add("records", jsonRecords);

		if (!measures.isEmpty())
			jsonResult.add("totals", getTotalsJson(totals, measures));

		if (!metadataFields.isEmpty())
			jsonResult.add("metadata", jsonMetadata);

		jsonResult.addProperty("count", count);

		return jsonResult.toString();
	}

	private static JsonArray getJsonArrayFromList(List<String> strings) {
		JsonArray jsonArray = new JsonArray();

		for (String s : strings) {
			jsonArray.add(new JsonPrimitive(s));
		}

		return jsonArray;
	}

	private JsonArray getRecordsJson(List results, List<String> dimensions, List<String> attributes,
	                                 List<String> measures, FieldGetter[] dimensionGetters,
	                                 FieldGetter[] attributeGetters, FieldGetter[] measureGetters,
	                                 KeyType[] keyTypes) {
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

		return jsonRecords;
	}

	private JsonObject getTotalsJson(TotalsPlaceholder totals, List<String> measures) {
		JsonObject jsonTotals = new JsonObject();

		for (String field : measures) {
			Object totalFieldValue = generateGetter(classLoader, totals.getClass(), field).get(totals);
			jsonTotals.add(field, new JsonPrimitive((Number) totalFieldValue));
		}

		return jsonTotals;
	}

	private JsonArray getDrillDownsJson(Set<DrillDown> drillDowns) {
		JsonArray jsonDrillDowns = new JsonArray();

		for (DrillDown drillDown : drillDowns) {
			JsonArray jsonDrillDownDimensions = new JsonArray();
			for (String drillDownDimension : drillDown.getChain()) {
				jsonDrillDownDimensions.add(new JsonPrimitive(drillDownDimension));
			}

			JsonArray jsonDrillDownMeasures = new JsonArray();
			for (String drillDownMeasure : drillDown.getMeasures()) {
				jsonDrillDownMeasures.add(new JsonPrimitive(drillDownMeasure));
			}

			JsonObject jsonDrillDown = new JsonObject();
			jsonDrillDown.add("dimensions", jsonDrillDownDimensions);
			jsonDrillDown.add("measures", jsonDrillDownMeasures);
			jsonDrillDowns.add(jsonDrillDown);
		}

		return jsonDrillDowns;
	}

	private JsonObject getFilterAttributesJson(List<String> filterAttributes, Object filterAttributesPlaceholder) {
		JsonObject jsonFilterAttributes = new JsonObject();
		for (String attribute : filterAttributes) {
			Object resolvedAttribute = generateGetter(classLoader, filterAttributesPlaceholder.getClass(), attribute)
					.get(filterAttributesPlaceholder);
			jsonFilterAttributes.add(attribute, resolvedAttribute == null ?
					null : new JsonPrimitive(resolvedAttribute.toString()));
		}
		return jsonFilterAttributes;
	}
}

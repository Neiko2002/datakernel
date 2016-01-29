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

package io.datakernel.cube.api;

import com.google.gson.*;
import com.google.gson.reflect.TypeToken;
import io.datakernel.aggregation_db.AggregationStructure;
import io.datakernel.aggregation_db.keytype.KeyType;

import java.lang.reflect.Type;
import java.util.*;

import static io.datakernel.cube.api2.HttpJsonConstants.*;

public class ReportingQueryResponseDeserializer implements JsonDeserializer<ReportingQueryResult> {
	private final AggregationStructure structure;
	private final ReportingConfiguration reportingConfiguration;

	public ReportingQueryResponseDeserializer(AggregationStructure structure, ReportingConfiguration reportingConfiguration) {
		this.structure = structure;
		this.reportingConfiguration = reportingConfiguration;
	}

	@Override
	public ReportingQueryResult deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext ctx)
			throws JsonParseException {
		JsonObject json = jsonElement.getAsJsonObject();

		int count = json.get(COUNT_FIELD).getAsInt();

		JsonArray jsonRecords = json.get(RECORDS_FIELD).getAsJsonArray();
		List<Map<String, Object>> records = new ArrayList<>(count);
		for (JsonElement jsonRecordElement : jsonRecords) {
			JsonObject jsonRecord = jsonRecordElement.getAsJsonObject();

			Map<String, Object> record = new LinkedHashMap<>();
			for (Map.Entry<String, JsonElement> jsonRecordEntry : jsonRecord.entrySet()) {
				String property = jsonRecordEntry.getKey();
				JsonElement propertyValue = jsonRecordEntry.getValue();

				KeyType keyType = structure.getKeyType(property);
				if (keyType != null)
					record.put(property, keyType.fromString(propertyValue.getAsString()));
				else if (structure.containsOutputField(property) || reportingConfiguration.containsComputedMeasure(property))
					record.put(property, propertyValue.getAsNumber());
				else if (reportingConfiguration.containsAttribute(property))
					record.put(property, propertyValue.getAsString());
				else
					throw new JsonParseException("Unknown property '" + property + "' in record");
			}
			records.add(record);
		}

		Type map = new TypeToken<Map<String, Object>>() {}.getType();
		JsonObject jsonTotals = json.get(TOTALS_FIELD).getAsJsonObject();
		Map<String, Object> totals = ctx.deserialize(jsonTotals, map);

		Type listOfStrings = new TypeToken<List<String>>() {}.getType();
		JsonObject jsonMetadata = json.get(METADATA_FIELD).getAsJsonObject();
		List<String> dimensions = ctx.deserialize(jsonMetadata.get(DIMENSIONS_FIELD), listOfStrings);
		List<String> measures = ctx.deserialize(jsonMetadata.get(MEASURES_FIELD), listOfStrings);
		List<String> attributes = ctx.deserialize(jsonMetadata.get(ATTRIBUTES_FIELD), listOfStrings);
		Map<String, Object> filterAttributes = ctx.deserialize(jsonMetadata.get(FILTER_ATTRIBUTES_FIELD), map);
		Set<List<String>> drillDowns = ctx.deserialize(jsonMetadata.get(DRILLDOWNS_FIELD), new TypeToken<Set<List<String>>>() {}.getType());

		return new ReportingQueryResult(records, totals, count, drillDowns, dimensions, attributes, measures, filterAttributes);
	}
}

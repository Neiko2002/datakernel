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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.datakernel.aggregation_db.AggregationQuery;
import io.datakernel.aggregation_db.gson.QueryOrderingGsonSerializer;
import io.datakernel.aggregation_db.gson.QueryPredicatesGsonSerializer;
import io.datakernel.async.ResultCallback;
import io.datakernel.codegen.utils.DefiningClassLoader;
import io.datakernel.cube.Cube;
import io.datakernel.cube.api2.HttpRequestHandler;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.*;

public final class CubeHttpServer {
	public static final String DIMENSIONS_REQUEST_PATH = "/dimensions/";
	public static final String OLD_QUERY_REQUEST_PATH = "/old_query/";
	public static final String INFO_REQUEST_PATH = "/info/";
	public static final String REPORTING_QUERY_REQUEST_PATH = "/reporting/";
	public static final String QUERY_REQUEST_PATH = "/";

	public static MiddlewareServlet createServlet(Cube cube, Eventloop eventloop, DefiningClassLoader classLoader) {
		final Gson gson = new GsonBuilder()
				.registerTypeAdapter(AggregationQuery.Predicates.class, new QueryPredicatesGsonSerializer(cube.getStructure()))
				.registerTypeAdapter(AggregationQuery.Ordering.class, new QueryOrderingGsonSerializer())
				.create();

		MiddlewareServlet servlet = new MiddlewareServlet();

		final HttpRequestHandler handler = new HttpRequestHandler(gson, cube, eventloop, classLoader);

		servlet.get(INFO_REQUEST_PATH, new InfoRequestHandler(cube, gson, classLoader));

		servlet.get(OLD_QUERY_REQUEST_PATH, new QueryHandler(gson, cube, eventloop, classLoader));

		servlet.get(DIMENSIONS_REQUEST_PATH, new DimensionsRequestHandler(gson, cube, eventloop, classLoader));

		servlet.get(REPORTING_QUERY_REQUEST_PATH, new ReportingQueryHandler(gson, cube, eventloop, classLoader));

		servlet.get(QUERY_REQUEST_PATH, new AsyncHttpServlet() {
			@Override
			public void serveAsync(HttpRequest request, ResultCallback<HttpResponse> callback) {
				handler.process(request, callback);
			}
		});

		return servlet;
	}

	public static AsyncHttpServer createServer(Cube cube, Eventloop eventloop, DefiningClassLoader classLoader) {
		return new AsyncHttpServer(eventloop, createServlet(cube, eventloop, classLoader));
	}

	public static AsyncHttpServer createServer(Cube cube, Eventloop eventloop, DefiningClassLoader classLoader, int port) {
		return createServer(cube, eventloop, classLoader).setListenPort(port);
	}
}

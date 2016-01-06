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

package io.datakernel.uikernel;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.datakernel.async.ResultCallback;
import io.datakernel.http.*;
import io.datakernel.http.server.AsyncHttpServlet;
import io.datakernel.util.ByteBufStrings;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

import static io.datakernel.http.HttpMethod.DELETE;
import static io.datakernel.http.HttpMethod.PUT;
import static io.datakernel.uikernel.Utils.deserializeUpdateRequest;

/**
 * Rest API for UiKernel Tables
 */
public class UiKernelServlets {
	private static final ContentType DEFAULT_CONTENT_TYPE_ENCODING = ContentType.of(MediaType.JSON, Charset.forName("UTF-8"));
	private static final int BAD_REQUEST = 400;
	private static final String ID_PARAMETER_NAME = "id";

	public static <K, R extends AbstractRecord<K>> MiddlewareServlet apiServlet(GridModel<K, R> model, Gson gson) {
		MiddlewareServlet main = new MiddlewareServlet();
		main.post(create(model, gson));
		main.get(read(model, gson));
		main.use(PUT, update(model, gson));
		main.use("/:" + ID_PARAMETER_NAME, DELETE, delete(model, gson));
		main.get("/:" + ID_PARAMETER_NAME, get(model, gson));
		return main;
	}

	public static <K, R extends AbstractRecord<K>> AsyncHttpServlet read(final GridModel<K, R> model, final Gson gson) {
		return new AsyncHttpServlet() {
			@Override
			public void serveAsync(HttpRequest req, final ResultCallback<HttpResponse> callback) {
				try {
					Map<String, String> parameters = req.getParameters();
					ReadSettings<K> settings = ReadSettings.of(gson, parameters);
					model.read(settings, new ResultCallback<ReadResponse<K, R>>() {
						@Override
						public void onResult(ReadResponse<K, R> response) {
							JsonObject json = response.toJson(gson, model.getRecordType(), model.getIdType());
							callback.onResult(HttpResponse.create()
									.setContentType(DEFAULT_CONTENT_TYPE_ENCODING)
									.body(ByteBufStrings.wrapUTF8(gson.toJson(json))));
						}

						@Override
						public void onException(Exception e) {
							callback.onResult(HttpResponse.notFound404());
						}
					});
				} catch (Exception e) {
					callback.onResult(HttpResponse.create(BAD_REQUEST));
				}
			}
		};
	}

	public static <K, R extends AbstractRecord<K>> AsyncHttpServlet get(final GridModel<K, R> model, final Gson gson) {
		return new AsyncHttpServlet() {
			@Override
			public void serveAsync(HttpRequest req, final ResultCallback<HttpResponse> callback) {
				try {
					Map<String, String> parameters = req.getParameters();
					ReadSettings<K> settings = ReadSettings.of(gson, parameters);
					K id = gson.fromJson(req.getUrlParameter(ID_PARAMETER_NAME), model.getIdType());
					model.read(id, settings, new ResultCallback<R>() {
						@Override
						public void onResult(R obj) {
							String json = gson.toJson(obj, model.getRecordType());
							callback.onResult(HttpResponse.create()
									.setContentType(DEFAULT_CONTENT_TYPE_ENCODING)
									.body(ByteBufStrings.wrapUTF8(json)));
						}

						@Override
						public void onException(Exception e) {
							callback.onResult(HttpResponse.notFound404());
						}
					});
				} catch (NumberFormatException e) {
					callback.onResult(HttpResponse.create(BAD_REQUEST));
				}
			}
		};
	}

	public static <K, R extends AbstractRecord<K>> AsyncHttpServlet create(final GridModel<K, R> model, final Gson gson) {
		return new AsyncHttpServlet() {
			@Override
			public void serveAsync(HttpRequest req, final ResultCallback<HttpResponse> callback) {
				try {
					String json = ByteBufStrings.decodeUTF8(req.getBody());
					R obj = gson.fromJson(json, model.getRecordType());
					model.create(obj, new ResultCallback<CreateResponse<K>>() {
						@Override
						public void onResult(CreateResponse<K> response) {
							JsonObject json = response.toJson(gson, model.getIdType());
							HttpResponse res = HttpResponse.create()
									.setContentType(DEFAULT_CONTENT_TYPE_ENCODING)
									.body(ByteBufStrings.wrapUTF8(gson.toJson(json)));
							callback.onResult(res);
						}

						@Override
						public void onException(Exception e) {
							callback.onResult(HttpResponse.notFound404());
						}
					});
				} catch (Exception e) {
					callback.onResult(HttpResponse.create(BAD_REQUEST));
				}
			}
		};
	}

	public static <K, R extends AbstractRecord<K>> AsyncHttpServlet update(final GridModel<K, R> model, final Gson gson) {
		return new AsyncHttpServlet() {
			@Override
			public void serveAsync(HttpRequest req, final ResultCallback<HttpResponse> callback) {
				try {
					String json = ByteBufStrings.decodeUTF8(req.getBody());
					List<R> list = deserializeUpdateRequest(gson, json, model.getRecordType(), model.getIdType());
					model.update(list, new ResultCallback<UpdateResponse<K, R>>() {
						@Override
						public void onResult(UpdateResponse<K, R> result) {
							JsonObject json = result.toJson(gson, model.getRecordType(), model.getIdType());
							callback.onResult(HttpResponse.create()
									.setContentType(DEFAULT_CONTENT_TYPE_ENCODING)
									.body(ByteBufStrings.wrapUTF8(gson.toJson(json))));
						}

						@Override
						public void onException(Exception e) {
							callback.onResult(HttpResponse.notFound404());
						}
					});
				} catch (Exception e) {
					callback.onResult(HttpResponse.create(BAD_REQUEST));
				}
			}
		};
	}

	public static <K, R extends AbstractRecord<K>> AsyncHttpServlet delete(final GridModel<K, R> model, final Gson gson) {
		return new AsyncHttpServlet() {
			@Override
			public void serveAsync(HttpRequest req, final ResultCallback<HttpResponse> callback) {
				try {
					K id = gson.fromJson(req.getUrlParameter("id"), model.getIdType());
					model.delete(id, new ResultCallback<DeleteResponse>() {
						@Override
						public void onResult(DeleteResponse response) {
							HttpResponse res = HttpResponse.create();
							if (response.hasErrors()) {
								JsonObject json = response.toJson(gson);
								res.setContentType(DEFAULT_CONTENT_TYPE_ENCODING)
										.body(ByteBufStrings.wrapUTF8(gson.toJson(json)));
							}
							callback.onResult(res);
						}

						@Override
						public void onException(Exception e) {
							callback.onResult(HttpResponse.notFound404());
						}
					});
				} catch (Exception e) {
					callback.onResult(HttpResponse.create(BAD_REQUEST));
				}
			}
		};
	}
}

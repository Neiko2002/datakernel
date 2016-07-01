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

package io.datakernel.http;

import io.datakernel.bytebufnew.ByteBufN;
import io.datakernel.bytebufnew.ByteBufNPool;
import io.datakernel.util.ByteBufStrings;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;

import static io.datakernel.http.HttpHeaders.of;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.*;

public class HttpMessageTest {
	public void assertHttpResponseEquals(String expected, HttpResponse result) {
		ByteBufN buf = result.write();
		assertEquals(expected, ByteBufStrings.decodeAscii(buf));
		buf.recycle();
	}

	public void assertHttpRequestEquals(String expected, HttpRequest request) {
		ByteBufN buf = request.write();
		assertEquals(expected, ByteBufStrings.decodeAscii(buf));
		buf.recycle();
	}

	@Test
	public void testHttpResponse() {
		assertHttpResponseEquals("HTTP/1.1 100 OK\r\nContent-Length: 0\r\n\r\n", HttpResponse.create(100));
		assertHttpResponseEquals("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n", HttpResponse.create(200));
		assertHttpResponseEquals("HTTP/1.1 400 Bad Request\r\nContent-Length: 77\r\n\r\n" +
				"Your browser (or proxy) sent a request that this server could not understand.", HttpResponse.create(400));
		assertHttpResponseEquals("HTTP/1.1 405 Error\r\nContent-Length: 0\r\n\r\n", HttpResponse.create(405));
		assertHttpResponseEquals("HTTP/1.1 500 Internal Server Error\r\nContent-Length: 81\r\n\r\n" +
				"The server encountered an internal error and was unable to complete your request.", HttpResponse.create(500));
		assertHttpResponseEquals("HTTP/1.1 502 Error\r\nContent-Length: 9\r\n\r\n" +
				"Error 502", HttpResponse.create(502).body("Error 502".getBytes(StandardCharsets.UTF_8)));
		assertHttpResponseEquals("HTTP/1.1 200 OK\r\nSet-Cookie: cookie1=\"value1\"\r\nContent-Length: 0\r\n\r\n",
				HttpResponse.create(200).setCookies(Collections.singletonList(new HttpCookie("cookie1", "value1"))));
		assertHttpResponseEquals("HTTP/1.1 200 OK\r\nSet-Cookie: cookie1=\"value1\", cookie2=\"value2\"\r\nContent-Length: 0\r\n\r\n",
				HttpResponse.create(200).setCookies(Arrays.asList(new HttpCookie("cookie1", "value1"), new HttpCookie("cookie2", "value2"))));
		assertHttpResponseEquals("HTTP/1.1 200 OK\r\nSet-Cookie: cookie1=\"value1\", cookie2=\"value2\"\r\nContent-Length: 0\r\n\r\n",
				HttpResponse.create(200).setCookies(asList(new HttpCookie("cookie1", "value1"), new HttpCookie("cookie2", "value2"))));
	}

	@Test
	public void testHttpRequest() {
		assertHttpRequestEquals("GET /index.html HTTP/1.1\r\nHost: test.com\r\n\r\n",
				HttpRequest.get("http://test.com/index.html"));
		assertHttpRequestEquals("POST /index.html HTTP/1.1\r\nHost: test.com\r\nContent-Length: 0\r\n\r\n",
				HttpRequest.post("http://test.com/index.html"));
		assertHttpRequestEquals("CONNECT /index.html HTTP/1.1\r\nHost: test.com\r\nContent-Length: 0\r\n\r\n",
				HttpRequest.create(HttpMethod.CONNECT).url("http://test.com/index.html"));
		assertHttpRequestEquals("GET /index.html HTTP/1.1\r\nHost: test.com\r\nCookie: cookie1=\"value1\"\r\n\r\n",
				HttpRequest.get("http://test.com/index.html").cookie(new HttpCookie("cookie1", "value1")));
		assertHttpRequestEquals("GET /index.html HTTP/1.1\r\nHost: test.com\r\nCookie: cookie1=\"value1\"; cookie2=\"value2\"\r\n\r\n",
				HttpRequest.get("http://test.com/index.html").cookies(asList(new HttpCookie("cookie1", "value1"), new HttpCookie("cookie2", "value2"))));

		HttpRequest request = HttpRequest.post("http://test.com/index.html");
		ByteBufN buf = ByteBufNPool.allocateAtLeast(100);
		buf.put("/abc".getBytes(), 0, 4);
		request.setBody(buf);
		assertHttpRequestEquals("POST /index.html HTTP/1.1\r\nHost: test.com\r\nContent-Length: 4\r\n\r\n/abc", request);
		buf.recycle();
	}

	private static String getHeaderValue(HttpMessage message, HttpHeader header) {
		return message.getHeader(header);
	}

	@Test
	public void testMultiHeaders() {
		HttpResponse h = HttpResponse.create(200);
		HttpHeader h1 = of("h1");
		HttpHeader h2 = of("h2");

		assertTrue(h.getHeaders().isEmpty());
		assertNull(getHeaderValue(h, h1));
		assertNull(getHeaderValue(h, h2));

		h.header(h1, "v1");
		h.header(h2, "v2");
		h.addHeader(h1, "v3");

		assertEquals(3, h.getHeaders().size());
		assertEquals(asList("v1", "v3"), h.getHeaderStrings(h1));
		assertEquals(singletonList("v2"), h.getHeaderStrings(h2));
		assertEquals("v2", h.getHeader(h2));
		assertEquals("v1", h.getHeader(h1));

		h.addHeader(h2, "v4");
		assertEquals(asList("v1", "v3"), h.getHeaderStrings(h1));
		assertEquals(asList("v2", "v4"), h.getHeaderStrings(h2));
		assertEquals("v2", getHeaderValue(h, h2));
		assertEquals("v1", getHeaderValue(h, h1));

	}
}
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

import io.datakernel.async.ParseException;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.util.ByteBufStrings;

import java.nio.charset.Charset;

import static io.datakernel.http.ContentTypes.lookup;
import static io.datakernel.http.HttpUtils.skipSpaces;
import static io.datakernel.util.ByteBufStrings.encodeAscii;

public final class ContentType {
	private static final byte[] CHARSET_KEY = encodeAscii("charset");

	final MediaType mime;
	final HttpCharset charset;

	ContentType(MediaType mime, HttpCharset charset) {
		this.mime = mime;
		this.charset = charset;
	}

	public static ContentType of(MediaType mime) {
		return new ContentType(mime, null);
	}

	public static ContentType of(MediaType mime, Charset charset) {
		return lookup(mime, HttpCharset.of(charset));
	}

	static ContentType parse(byte[] bytes, int pos, int length) throws ParseException {
		try {
			// parsing media type
			int end = pos + length;

			pos = skipSpaces(bytes, pos, end);
			int start = pos;
			int lowerCaseHashCode = 1;
			while (pos < end && bytes[pos] != ';') {
				byte b = bytes[pos];
				if (b >= 'A' && b <= 'Z') {
					b += 'a' - 'A';
				}
				lowerCaseHashCode = lowerCaseHashCode * 31 + b;
				pos++;
			}
			MediaType type = MediaTypes.of(bytes, start, pos - start, lowerCaseHashCode);
			pos++;

			// parsing parameters if any (interested in 'charset' only)
			HttpCharset charset = null;
			if (pos < end) {
				pos = skipSpaces(bytes, pos, end);
				start = pos;
				while (pos < end) {
					if (bytes[pos] == '=' && ByteBufStrings.equalsLowerCaseAscii(CHARSET_KEY, bytes, start, pos - start)) {
						pos++;
						start = pos;
						while (pos < end && bytes[pos] != ';') {
							pos++;
						}
						charset = HttpCharset.parse(bytes, start, pos - start);
					} else if (bytes[pos] == ';' && pos + 1 < end) {
						start = skipSpaces(bytes, pos + 1, end);
					}
					pos++;
				}
			}
			return lookup(type, charset);
		} catch (RuntimeException e) {
			throw new ParseException();
		}
	}

	static void render(ContentType type, ByteBuf buf) {
		int pos = render(type, buf.array(), buf.tail());
		buf.tail(pos);
	}

	static int render(ContentType type, byte[] container, int pos) {
		pos += MediaTypes.render(type.getMediaType(), container, pos);
		if (type.charset != null) {
			container[pos++] = ';';
			container[pos++] = ' ';
			System.arraycopy(CHARSET_KEY, 0, container, pos, CHARSET_KEY.length);
			pos += CHARSET_KEY.length;
			container[pos++] = '=';
			pos += HttpCharset.render(type.charset, container, pos);
		}
		return pos;
	}

	public Charset getCharset() {
		return charset == null ? null : charset.toJavaCharset();
	}

	public MediaType getMediaType() {
		return mime;
	}

	int size() {
		int size = mime.size();
		if (charset != null) {
			size += charset.size();
			size += 10; // '; charset='
		}
		return size;
	}

	@Override
	public String toString() {
		return "ContentType{" +
				"type=" + mime +
				", charset=" + charset +
				'}';
	}
}

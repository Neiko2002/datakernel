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

package io.datakernel.bytebufnew;

import static java.lang.System.arraycopy;

public final class ByteBufQueue {
	private static final int DEFAULT_CAPACITY = 8;

	private ByteBufN[] bufs;

	private int first = 0;
	private int last = 0;

	public ByteBufQueue(int capacity) {
		this.bufs = new ByteBufN[capacity];
	}

	public ByteBufQueue() {
		this(DEFAULT_CAPACITY);
	}

	private int next(int i) {
		return ++i >= bufs.length ? 0 : i;
	}

	private void doPoll() {
		bufs[first].recycle();
		first = next(first);
	}

	private void grow() {
		ByteBufN[] newBufs = new ByteBufN[bufs.length * 2];
		arraycopy(bufs, last, newBufs, 0, bufs.length - last);
		arraycopy(bufs, 0, newBufs, bufs.length - last, last);
		first = 0;
		last = bufs.length;
		bufs = newBufs;
	}

	public void add(ByteBufN buf) {
		if (!buf.canRead()) {
			buf.recycle();
			return;
		}

		bufs[last] = buf;
		last = next(last);
		if (last == first) {
			grow();
		}
	}

	public void addAll(Iterable<ByteBufN> byteBufs) {
		for (ByteBufN buf : byteBufs) {
			add(buf);
		}
	}

	public ByteBufN take() {
		assert hasRemaining();
		ByteBufN buf = bufs[first];
		first = next(first);
		return buf;
	}

	/**
	 * Creates and returns ByteBufSlice that contains {@code maxSize} bytes from queue's first ByteBuf
	 * if latter contains enough bytes. Otherwise creates and returns ByteBuf that contains all bytes
	 * from first ByteBuf in queue.
	 *
	 * @param maxSize number of bytes to returning
	 * @return ByteBuf with result bytes
	 */
	public ByteBufN takeMaxSize(int maxSize) {
		assert hasRemaining();
		ByteBufN buf = bufs[first];
		if (maxSize >= buf.remainingToRead()) {
			first = next(first);
			return buf;
		}
		ByteBufN result = buf.slice(buf.getReadPosition(), maxSize);
		buf.skip(maxSize);
		return result;
	}

	/**
	 * Returns ByteBuf that contains {@code exactSize} of bytes if queue has enough bytes.
	 * Otherwise returns ByteBuf that contains all bytes from queue
	 *
	 * @param exactSize amount of bytes to return
	 * @return ByteBuf with {@code exactSize} or less bytes
	 */
	public ByteBufN takeExactSize(int exactSize) {
		if (!hasRemaining())
			return ByteBufN.empty();
		ByteBufN buf = bufs[first];
		if (buf.remainingToRead() == exactSize) {
			first = next(first);
			return buf;
		} else if (exactSize < buf.remainingToRead()) {
			ByteBufN result = buf.slice(buf.getReadPosition(), exactSize);
			buf.skip(exactSize);
			return result;
		}
		ByteBufN result = ByteBufNPool.allocateAtLeast(exactSize);
		drainTo(result.array(), 0, exactSize);
		return result;
	}

	/**
	 * Creates and returns ByteBuf with all remaining bytes from queue
	 *
	 * @return ByteBuf with all remaining bytes
	 */
	public ByteBufN takeRemaining() {
		return takeExactSize(remainingBytes());
	}

	/**
	 * Returns the first ByteBuf from this queue
	 */
	public ByteBufN peekBuf() {
		return hasRemaining() ? bufs[first] : null;
	}

	/**
	 * Returns the ByteBuf with the given index,  relatively than head of queue
	 *
	 * @param n index of ByteBuf relatively than head of queue
	 */
	public ByteBufN peekBuf(int n) {
		assert n <= remainingBufs();
		int i = first + n;
		if (i >= bufs.length)
			i -= bufs.length;
		return bufs[i];
	}

	/**
	 * Returns the number of ByteBufs in this queue
	 */
	public int remainingBufs() {
		return last >= first ? last - first : bufs.length + (last - first);
	}

	/**
	 * Returns the number of bytes in this queue
	 */
	public int remainingBytes() {
		int result = 0;
		for (int i = first; i != last; i = next(i)) {
			result += bufs[i].remainingToRead();
		}
		return result;
	}

	/**
	 * Tells whether or not this queue is empty.
	 *
	 * @return true if, and only if, there is at least one element is remaining in this queue
	 */
	public boolean isEmpty() {
		return !hasRemaining();
	}

	public boolean hasRemaining() {
		return first != last;
	}

	/**
	 * Tells whether or not this queue has remaining bytes.
	 *
	 * @param remaining number of bytes for checking
	 * @return true if, and only if, there are remaining bytes.
	 */
	public boolean hasRemainingBytes(int remaining) {
		for (int i = first; i != last; i = next(i)) {
			int bufRemaining = bufs[i].remainingToRead();
			if (bufRemaining >= remaining)
				return true;
			remaining -= bufRemaining;
		}
		return false;
	}

	/**
	 * Returns the first byte from this queue and removes it.
	 */
	public byte getByte() {
		assert hasRemaining();
		ByteBufN buf = bufs[first];
		assert buf.canRead();
		byte result = buf.get();
		if (!buf.canRead()) {
			doPoll();
		}
		return result;
	}

	/**
	 * Returns the first byte from this queue without its removing.
	 */
	public byte peekByte() {
		assert hasRemaining();
		ByteBufN buf = bufs[first];
		return buf.peek();
	}

	/**
	 * Returns the byte from this queue with the given index
	 *
	 * @param index the index at which the bytes will be returned
	 */
	public byte peekByte(int index) {
		assert hasRemainingBytes(index + 1);
		for (int i = first; ; i = next(i)) {
			ByteBufN buf = bufs[i];
			if (index < buf.remainingToRead())
				return buf.peek(index);
			index -= buf.remainingToRead();
		}
	}

	/**
	 * Removes {@code maxSize} bytes from this queue
	 *
	 * @param maxSize number of bytes for removing
	 * @return number of removed bytes
	 */
	public int advance(int maxSize) {
		int s = maxSize;
		while (hasRemaining()) {
			ByteBufN buf = bufs[first];
			int remaining = buf.remainingToRead();
			if (s < remaining) {
				buf.skip(s);
				return maxSize;
			} else {
				buf.setReadPosition(buf.getWritePosition());
				doPoll();
				s -= remaining;
			}
		}
		return maxSize - s;
	}

	/**
	 * Adds {@code maxSize} bytes from this queue to dest if queue contains more than {@code maxSize} bytes.
	 * Otherwise adds all bytes from queue to dest. In both cases advances queue's position to number of drained bytes.
	 *
	 * @param dest       array to draining
	 * @param destOffset start position for adding to dest
	 * @param maxSize    number of bytes for adding
	 * @return number of drained bytes.
	 */
	public int drainTo(byte[] dest, int destOffset, int maxSize) {
		int s = maxSize;
		while (hasRemaining()) {
			ByteBufN buf = bufs[first];
			int remaining = buf.remainingToRead();
			if (s < remaining) {
				arraycopy(buf.array(), buf.getReadPosition(), dest, destOffset, s);
				buf.setReadPosition(buf.getReadPosition() + s);
				return maxSize;
			} else {
				arraycopy(buf.array(), buf.getReadPosition(), dest, destOffset, remaining);
				buf.setReadPosition(buf.getWritePosition());
				doPoll();
				s -= remaining;
				destOffset += remaining;
			}
		}
		return maxSize - s;
	}

	/**
	 * Adds {@code maxSize} bytes from this queue to ByteBuf dest if queue contains more than {@code maxSize} bytes.
	 * Otherwise adds all bytes from queue to dest. In both cases advances queue's position to number of drained bytes.
	 *
	 * @param dest    ByteBuf for draining
	 * @param maxSize number of bytes for adding
	 * @return number of drained bytes.
	 */
	public int drainTo(ByteBufN dest, int maxSize) {
		int actualSize = drainTo(dest.array(), dest.getWritePosition(), maxSize);
		dest.skip(actualSize);
		return actualSize;
	}

	/**
	 * Adds as much bytes to dest as it can store. If queue doesn't contain enough bytes - adds all byte from queue.
	 * Advances queue's position to number of drained bytes.
	 *
	 * @param dest ByteBuf for draining
	 * @return number of drained bytes
	 */
	public int drainTo(ByteBufN dest) {
		return drainTo(dest, dest.remainingToWrite());
	}

	/**
	 * Copies all bytes from this queue to dest, and removes it from this queue.
	 *
	 * @param dest ByteBufQueue  for draining
	 * @return number of adding bytes
	 */
	public int drainTo(ByteBufQueue dest) {
		int size = 0;
		while (hasRemaining()) {
			ByteBufN buf = take();
			dest.add(buf);
			size += buf.remainingToRead();
		}
		return size;
	}

	/**
	 * Adds to ByteBufQueue dest {@code maxSize} bytes from this queue. If this queue doesn't contain enough bytes,
	 * add all bytes from this queue.
	 *
	 * @param dest    ByteBufQueue for draining
	 * @param maxSize number of bytes for adding
	 * @return number of added elements
	 */
	public int drainTo(ByteBufQueue dest, int maxSize) {
		int s = maxSize;
		while (s != 0 && hasRemaining()) {
			ByteBufN buf = takeMaxSize(s);
			dest.add(buf);
			s -= buf.remainingToRead();
		}
		return maxSize - s;
	}

	/**
	 * Recycles all ByteBufs from this queue.
	 */
	public void clear() {
		for (int i = first; i != last; i = next(i)) {
			bufs[i].recycle();
		}
		first = last = 0;
	}

	@Override
	public String toString() {
		return "bufs:" + remainingBufs() + " bytes:" + remainingBytes();
	}
}

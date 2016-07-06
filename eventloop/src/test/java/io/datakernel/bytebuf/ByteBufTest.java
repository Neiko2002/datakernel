package io.datakernel.bytebuf;

import io.datakernel.util.ByteBufStrings;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static io.datakernel.bytebuf.ByteBufPool.*;
import static org.junit.Assert.*;

public class ByteBufTest {
	private static final byte[] BYTES = new byte[]{'T', 'e', 's', 't', ' ', 'm', 'e', 's', 's', 'a', 'g', 'e'};

	@Before
	public void setUp() {
		ByteBufPool.clear();
		ByteBufPool.setSizes(32, 1 << 30);
	}

	@Test
	public void testSlice() {
		byte[] bytes = ByteBufStrings.encodeAscii("Hello, World");
		ByteBuf buf = ByteBuf.wrapForReading(bytes);

		ByteBuf slice = buf.slice(7, 5);

		assertFalse(buf == slice);
		assertEquals("World", slice.toString());

		buf = createEmptyByteBufOfSize(16);
		buf.put(bytes);

		slice = buf.slice();

		assertFalse(buf == slice);
		assertEquals("Hello, World", slice.toString());
	}

	@Test
	public void testEditing() {
		ByteBuf buf = createEmptyByteBufOfSize(256);
		assertEquals(0, buf.getReadPosition());

		buf.put((byte) 'H');
		assertEquals(1, buf.getWritePosition());
		assertEquals('H', buf.at(0));

		buf.put(new byte[]{'e', 'l', 'l', 'o'});
		buf.put(new byte[]{';', ' ', ',', ' ', '.', ' ', '!', ' '}, 2, 4);
		assertEquals(7, buf.getWritePosition());

		ByteBuf worldBuf = ByteBuf.wrapForReading(new byte[]{'W', 'o', 'r', 'l', 'd', '!'});
		buf.put(worldBuf);

		assertEquals(worldBuf.getLimit(), worldBuf.getReadPosition());
		assertFalse(worldBuf.canWrite());
		assertEquals(13, buf.getWritePosition());

		ByteBuf slice = buf.slice();
		ByteBuf newBuf = createEmptyByteBufOfSize(slice.getLimit());
		slice.drainTo(newBuf, 10);
		assertEquals(10, slice.getReadPosition());
		assertEquals(10, newBuf.getWritePosition());

		slice.drainTo(newBuf, 3);

		assertEquals("Hello, World!", newBuf.toString());
	}

	@Test
	public void transformsToByteBufferInReadMode() {
		ByteBuf buf = createEmptyByteBufOfSize(8);
		buf.setWritePosition(5);
		buf.setReadPosition(2);

		ByteBuffer buffer = buf.toByteBufferInReadMode();

		assertEquals(2, buffer.position());
		assertEquals(5, buffer.limit());
	}

	@Test
	public void transformsToByteBufferInWriteMode() {
		ByteBuf buf = createEmptyByteBufOfSize(8);
		buf.setWritePosition(5);
		buf.setReadPosition(2);

		ByteBuffer buffer = buf.toByteBufferInWriteMode();

		assertEquals(5, buffer.position());
		assertEquals(8, buffer.limit());
	}

	@Test
	public void testPoolAndRecycleMechanism() {
		int size = 500;
		ByteBuf buf = ByteBufPool.allocate(size);
		assertNotEquals(size, buf.getLimit()); // {expected to create 2^N sized bufs only, 500 not in {a}|a == 2^N } => size != limit
		assertEquals(512, buf.getLimit());

		buf.put(BYTES);

		buf.recycle();

		try {
			buf.put((byte) 'a');
		} catch (AssertionError e) {
			assertEquals(AssertionError.class, e.getClass());
		}

		buf = ByteBufPool.allocate(300);
		buf.setWritePosition(BYTES.length);
		byte[] bytes = new byte[BYTES.length];
		buf.drainTo(bytes, 0, bytes.length);
		assertArrayEquals(bytes, BYTES);

		buf.recycle();
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testSliceAndRecycleMechanism() {
		ByteBuf buf = ByteBufPool.allocate(5);
		ByteBuf slice0 = buf.slice(1, 3);
		ByteBuf slice01 = slice0.slice(2, 1);
		ByteBuf slice1 = buf.slice(4, 1);

		assertTrue(buf.canWrite());
		slice1.recycle();
		assertTrue(buf.canWrite());
		slice0.recycle();
		assertTrue(buf.canWrite());
		slice01.recycle();
	}

	@Test
	public void testViews() {
		// emulate engine that receives randomly sized bufs from `net` and sends them to some `consumer`

		class MockConsumer {
			private int i = 0;

			private void consume(ByteBuf buf) {
				assertEquals("Test message " + i++, buf.toString());
				buf.recycle();
			}
		}

		MockConsumer consumer = new MockConsumer();
		for (int i = 0; i < 100; i++) {
			ByteBuf buf = ByteBufPool.allocate(32);
			ByteBuffer buffer = buf.toByteBufferInWriteMode();
			buffer.put(("Test message " + i).getBytes());
			buffer.flip();
			buf.setWritePosition(buffer.limit());
			buf.setReadPosition(buffer.position());
			consumer.consume(buf.slice());
			buf.recycle();
		}

		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testConcat() {
		ByteBuf buf = ByteBufPool.allocate(64);
		buf.put(BYTES);

		ByteBuf secondBuf = ByteBufPool.allocate(32);
		secondBuf.put(BYTES);

		buf = ByteBufPool.concat(buf, secondBuf.slice());
		buf = ByteBufPool.concat(buf, secondBuf.slice());
		buf = ByteBufPool.concat(buf, secondBuf.slice());
		buf = ByteBufPool.concat(buf, secondBuf.slice());
		buf = ByteBufPool.concat(buf, secondBuf.slice());

		assertEquals(new String(BYTES)
				+ new String(BYTES)
				+ new String(BYTES)
				+ new String(BYTES)
				+ new String(BYTES)
				+ new String(BYTES), buf.toString());

		buf.recycle();
		secondBuf.recycle();
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testSkipAndAdvance() {
		ByteBuf buf = createEmptyByteBufOfSize(5);
		buf.put(new byte[]{'a', 'b', 'c'});
		buf.skip(2);
		assertEquals('c', buf.get());
		buf.array[3] = 'd';
		buf.array[4] = 'e';
		assertFalse(buf.canRead());
		buf.advance(2);
		assertTrue(buf.canRead());
		byte[] bytes = new byte[2];
		buf.drainTo(bytes, 0, 2);
		assertTrue(Arrays.equals(new byte[]{'d', 'e'}, bytes));
	}

	@Test
	public void testGet() {
		ByteBuf buf = createEmptyByteBufOfSize(3);
		buf.put(new byte[]{'a', 'b', 'c'});

		assertEquals('a', buf.get());
		assertEquals('b', buf.get());
		assertEquals('c', buf.get());
	}

	private ByteBuf createEmptyByteBufOfSize(int size) {
		return ByteBuf.wrapForWriting(new byte[size]);
	}
}
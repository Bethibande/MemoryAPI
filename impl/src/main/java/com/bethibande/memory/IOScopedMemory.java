package com.bethibande.memory;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemoryAddress;
import jdk.incubator.foreign.MemoryLayout;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;

import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;

class IOScopedMemory implements IOAccessible {

    private static final byte ONE = 0x01;
    private static final byte ZERO = 0x00;

    public static IOScopedMemory mapFile(final Path path,
                                         final long offset,
                                         final long size,
                                         final FileChannel.MapMode mode) throws IOException {
        if(path == null || mode == null) throw new NullPointerException("path and mode must not be null.");
        if(!path.toFile().isFile()) throw new IllegalArgumentException("The specified path must be a file.");

        final ResourceScope scope = ResourceScope.newConfinedScope();
        final MemorySegment segment = MemorySegment.mapFile(path, offset, size, mode, scope);

        return new IOScopedMemory(scope, segment);
    }

    public static IOScopedMemory allocateAlignedNative(final Object _layout) {
        if(!(_layout instanceof MemoryLayout layout)) throw new IllegalArgumentException("layout must be an instance of jdk.incubator.foreign.MemoryLayout.");

        final ResourceScope scope = ResourceScope.newConfinedScope();
        return new IOScopedMemory(scope, MemorySegment.allocateNative(layout, scope));
    }

    public static IOScopedMemory atAddress(final long addr, final long size) {
        final MemoryAddress address = MemoryAddress.ofLong(addr);
        final ResourceScope scope = ResourceScope.newConfinedScope();
        return new IOScopedMemory(scope, address.asSegment(size, scope));
    }

    public static IOScopedMemory allocateNative(final long size) {
        final ResourceScope scope = ResourceScope.newConfinedScope();
        return new IOScopedMemory(scope, MemorySegment.allocateNative(size, scope));
    }

    public static IOScopedMemory allocateAlignedNative(final long size, final long alignment) {
        final ResourceScope scope = ResourceScope.newConfinedScope();
        return new IOScopedMemory(scope, MemorySegment.allocateNative(size, alignment, scope));
    }

    private final ResourceScope scope;
    private final MemorySegment segment;

    private ByteOrder order = ByteOrder.nativeOrder();
    private AtomicLong index;

    public IOScopedMemory(final ResourceScope scope, final MemorySegment segment) {
        this.scope = scope;
        this.segment = segment;
    }

    @Override
    public void setByteOrder(final ByteOrder order) {
        this.order = order;
    }

    @Override
    public boolean canSlice() {
        return true;
    }

    @Override
    public IOAccessible slice(final long index, final long length) {
        return new IOScopedMemory(scope, segment.asSlice(index, length));
    }

    public byte readByte() {
        return getByte(index.getAndIncrement());
    }

    public short readShort() {
        return getShort(index.getAndAdd(2));
    }

    public int readInt() {
        return getInt(index.getAndAdd(4));
    }

    public long readLong() {
        return getLong(index.getAndAdd(8));
    }

    public float readFloat() {
        return getFloat(index.getAndAdd(4));
    }

    public double readDouble() {
        return getDouble(index.getAndAdd(8));
    }

    public char readChar() {
        return getChar(index.getAndAdd(2));
    }

    public boolean readBoolean() {
        return getBoolean(index.getAndAdd(1));
    }

    public void writeByte(final byte b) {
        setByte(index.getAndIncrement(), b);
    }

    public void writeShort(final short s) {
        setShort(index.getAndAdd(2), s);
    }

    public void writeInt(final int i) {
        setInt(index.getAndAdd(4), i);
    }

    public void writeLong(final long l) {
        setLong(index.getAndAdd(8), l);
    }

    public void writeFloat(final float f) {
        setFloat(index.getAndAdd(4), f);
    }

    public void writeDouble(final double d) {
        setDouble(index.getAndAdd(8), d);
    }

    public void writeChar(final char c) {
        setChar(index.getAndAdd(2), c);
    }

    public void writeBoolean(final boolean b) {
        setBoolean(index.getAndIncrement(), b);
    }

    public void setByte(final long index, final byte b) {
        MemoryAccess.setByteAtOffset(segment, index, b);
    }

    public void setShort(final long index, final short s) {
        MemoryAccess.setShortAtOffset(segment, index, order, s);
    }

    public void setInt(final long index, final int i) {
        MemoryAccess.setIntAtOffset(segment, index, order, i);
    }

    public void setLong(final long index, final long l) {
        MemoryAccess.setLongAtOffset(segment, index, order, l);
    }

    public void setFloat(final long index, final float f) {
        MemoryAccess.setFloatAtOffset(segment, index, order, f);
    }

    public void setDouble(final long index, final double d) {
        MemoryAccess.setDoubleAtOffset(segment, index, order, d);
    }

    public void setBoolean(final long index, final boolean b) {
        MemoryAccess.setByteAtOffset(segment, index, b ? ONE: ZERO);
    }

    public void setChar(final long index, final char c) {
        MemoryAccess.setCharAtOffset(segment, index, c);
    }

    public byte getByte(final long index) {
        return MemoryAccess.getByteAtOffset(segment, index);
    }

    public short getShort(final long index) {
        return MemoryAccess.getShortAtOffset(segment, index, order);
    }

    public int getInt(final long index) {
        return MemoryAccess.getIntAtOffset(segment, index, order);
    }

    public long getLong(final long index) {
        return MemoryAccess.getLongAtOffset(segment, index, order);
    }

    public float getFloat(final long index) {
        return MemoryAccess.getFloatAtOffset(segment, index, order);
    }

    public double getDouble(final long index) {
        return MemoryAccess.getDoubleAtOffset(segment, index, order);
    }

    public boolean getBoolean(final long index) {
        return MemoryAccess.getByteAtOffset(segment, index) == ONE;
    }

    public char getChar(final long index) {
        return MemoryAccess.getCharAtOffset(segment, index, order);
    }

    @Override
    public void setIndex(final long index) {
        this.index = new AtomicLong(index);
    }

    @Override
    public void skip(final long bytes) {
        index.addAndGet(bytes);
    }

    @Override
    public void flush() {
        throw new UnsupportedOperationException("IOScopedMemory does not support flush operations");
    }

    @Override
    public void release() {
        scope.close();
    }

    @Override
    public byte read() {
        throw new UnsupportedOperationException("IOScopedMemory only supports bulk read/write operations");
    }

    @Override
    public byte[] read(final int len) {
        final MemorySegment slice = segment.asSlice(index.getAndAdd(len), len);
        return slice.toByteArray();
    }

    @Override
    public byte get(final long index) {
        throw new UnsupportedOperationException("IOScopedMemory only supports bulk read/write operations");
    }

    @Override
    public byte[] get(final long index, final int length) {
        final MemorySegment slice = segment.asSlice(index, length);
        return slice.toByteArray();
    }

    @Override
    public void write(final byte b) {
        throw new UnsupportedOperationException("IOScopedMemory only supports bulk read/write operations");
    }

    @Override
    public void write(final byte[] data, final int off, final int len) {
        final MemorySegment slice = segment.asSlice(index.getAndAdd(len), len);
        final byte[] dataCopy = off == 0L ? data : new byte[len];
        if(off != 0 || len != data.length) System.arraycopy(data, off, dataCopy, 0, len);
        slice.copyFrom(MemorySegment.ofArray(dataCopy));
    }

    @Override
    public void set(final byte b, final long index) {
        throw new UnsupportedOperationException("IOScopedMemory only supports bulk read/write operations");
    }

    @Override
    public void set(final byte[] b, final long index, final int off, final int len) {
        final MemorySegment slice = segment.asSlice(index, len);
        final byte[] dataCopy = off == 0L ? b : new byte[len];
        if(off != 0 || len != b.length) System.arraycopy(b, off, dataCopy, 0, len);
        slice.copyFrom(MemorySegment.ofArray(dataCopy));
    }

    @Override
    public void copyFrom(final IOAccessible accessible, final long index, final long offset, final int length) {
        if(accessible instanceof IOScopedMemory memory) {
            final MemorySegment origin = memory.segment.asSlice(offset, length);
            final MemorySegment slice = segment.asSlice(index, length);
            slice.copyFrom(origin);
            return;
        }

        final MemorySegment buffered = MemorySegment.ofArray(accessible.get(offset, length));
        final MemorySegment slice = segment.asSlice(index, length);
        slice.copyFrom(buffered);
    }

    @Override
    public void copyFrom(final IOAccessible accessible, final long offset, final int length) {
        if(accessible instanceof IOScopedMemory memory) {
            final MemorySegment origin = memory.segment.asSlice(offset, length);
            final MemorySegment slice = segment.asSlice(index.getAndAdd(length), length);
            slice.copyFrom(origin);

            return;
        }

        final MemorySegment buffered = MemorySegment.ofArray(accessible.get(offset, length));
        final MemorySegment slice = segment.asSlice(index.getAndAdd(length), length);
        slice.copyFrom(buffered);
    }

    @Override
    public void copyFrom(final IOAccessible accessible, final int length) {
        if(accessible instanceof IOScopedMemory memory) {
            final MemorySegment origin = memory.segment.asSlice(memory.index.getAndAdd(length), length);
            final MemorySegment slice = segment.asSlice(index.getAndAdd(length), length);
            slice.copyFrom(origin);

            return;
        }

        final MemorySegment buffered = MemorySegment.ofArray(accessible.read(length));
        final MemorySegment slice = segment.asSlice(index.getAndAdd(length), length);
        slice.copyFrom(buffered);

    }
}

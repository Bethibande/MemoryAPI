package com.bethibande.memory;

import jdk.incubator.foreign.MemoryLayout;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;

class IOScopedMemory implements IOAccessible {

    public static IOScopedMemory allocateAlignedNative(final MemoryLayout layout) {
        final ResourceScope scope = ResourceScope.newConfinedScope();
        return new IOScopedMemory(scope, MemorySegment.allocateNative(layout, scope));
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

    private long index;

    public IOScopedMemory(final ResourceScope scope, final MemorySegment segment) {
        this.scope = scope;
        this.segment = segment;
    }

    @Override
    public boolean canSlice() {
        return true;
    }

    @Override
    public IOAccessible slice(final long index, final long length) {
        return new IOScopedMemory(scope, segment.asSlice(index, length));
    }

    @Override
    public void setIndex(final long index) {
        this.index = index;
    }

    @Override
    public void skip(final long bytes) {
        index += bytes;
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
        final MemorySegment slice = segment.asSlice(index, len);
        index += len;
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
        final MemorySegment slice = segment.asSlice(index, len);
        final byte[] dataCopy = off == 0L ? data : new byte[len];
        if(off != 0) System.arraycopy(data, off, dataCopy, 0, len);
        this.index += len;
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
        if(off != 0) System.arraycopy(b, off, dataCopy, 0, len);
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
            final MemorySegment slice = segment.asSlice(index, length);
            slice.copyFrom(origin);

            this.index += length;

            return;
        }

        final MemorySegment buffered = MemorySegment.ofArray(accessible.get(offset, length));
        final MemorySegment slice = segment.asSlice(index, length);
        slice.copyFrom(buffered);

        this.index += length;
    }

    @Override
    public void copyFrom(final IOAccessible accessible, final int length) {
        if(accessible instanceof IOScopedMemory memory) {
            final MemorySegment origin = memory.segment.asSlice(memory.index, length);
            final MemorySegment slice = segment.asSlice(index, length);
            slice.copyFrom(origin);

            this.index += length;
            memory.index += length;

            return;
        }

        final MemorySegment buffered = MemorySegment.ofArray(accessible.read(length));
        final MemorySegment slice = segment.asSlice(index, length);
        slice.copyFrom(buffered);

        this.index += length;
    }
}

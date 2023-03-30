package com.bethibande.memory.impl;

import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;

public class IOBuffer implements IOAccessible {

    private byte[] buffer;
    private int index;

    public IOBuffer(final byte[] buffer) {
        this.buffer = buffer;
    }

    @Override
    public boolean canSlice() {
        return true;
    }

    @Override
    public IOAccessible slice(final long index, final long length) {
        final ResourceScope scope = ResourceScope.globalScope();
        final MemorySegment segment = MemorySegment.ofArray(buffer);
        return new IOScopedMemory(scope, segment.asSlice(index, length));
    }

    @Override
    public void setIndex(final long index) {
        this.index =  (int) index;
    }

    @Override
    public void skip(final long bytes) {
        this.index += (int) bytes;
    }

    @Override
    public void flush() {
        throw new UnsupportedOperationException("Cannot flush a byte buffer");
    }

    @Override
    public void release() {
        this.buffer = null;
        this.index = 0;
    }

    @Override
    public byte read() {
        return buffer[index++];
    }

    @Override
    public byte[] read(final int len) {
        final byte[] arr = new byte[len];
        System.arraycopy(buffer, index, arr, 0, len);
        index += len;
        return arr;
    }

    @Override
    public byte get(final long index) {
        return buffer[(int)index];
    }

    @Override
    public byte[] get(final long index, final int length) {
        final byte[] arr = new byte[length];
        System.arraycopy(buffer, (int) index, arr, 0, length);
        return arr;
    }

    @Override
    public void write(final byte b) {
        buffer[index++] = b;
    }

    @Override
    public void write(final byte[] data, final int off, final int len) {
        System.arraycopy(data, off, buffer, index, len);
    }

    @Override
    public void set(final byte b, final long index) {
        buffer[(int) index] = b;
    }

    @Override
    public void set(final byte[] b, final long index, final int off, final int len) {
        System.arraycopy(b, off, buffer, (int) index, len);
    }

    @Override
    public void copyFrom(final IOAccessible accessible, final long index, final long offset, final int length) {
        if(accessible instanceof IOBuffer ioBuffer) {
            System.arraycopy(ioBuffer.buffer, (int) offset, buffer, (int) index, length);
            return;
        }

        final byte[] data = accessible.get(offset, length);
        System.arraycopy(data, 0, buffer, (int) index, length);
    }

    @Override
    public void copyFrom(final IOAccessible accessible, final long offset, final int length) {
        if(accessible instanceof IOBuffer ioBuffer) {
            System.arraycopy(ioBuffer.buffer, (int) offset, buffer, index, length);
            this.index += length;
            return;
        }

        final byte[] data = accessible.get(offset, length);
        System.arraycopy(data, 0, buffer, index, length);
        this.index += length;
    }

    @Override
    public void copyFrom(final IOAccessible accessible, final int length) {
        if(accessible instanceof IOBuffer ioBuffer) {
            System.arraycopy(ioBuffer.buffer, ioBuffer.index, buffer, index, length);

            this.index += length;
            ioBuffer.index += length;

            return;
        }

        final byte[] data = accessible.read(length);
        System.arraycopy(data, 0, buffer, index, length);

        this.index += length;
    }
}
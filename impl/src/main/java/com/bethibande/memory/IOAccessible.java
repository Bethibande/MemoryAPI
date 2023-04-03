package com.bethibande.memory;

import java.nio.ByteOrder;

/**
 * This class represents unsafe access to memory, streams and more.
 * This class does not validate read and write calls, all read and write calls,
 * if supported by the underlying target (like byte-array, stream, [...]) are executed,
 * regardless of index bounds or access rights.
 * Whether read/write or indexed get/set operations are available or index bounds are to be validated
 * by all classes using this class
 */
interface IOAccessible {

    default void setByteOrder(final ByteOrder order) { }

    default boolean canSlice() {
        return false;
    }

    default IOAccessible slice(final long index, final long length) {
        return null;
    }

    void setIndex(final long index);
    void skip(final long bytes);

    void flush();
    void release();

    byte read();
    byte[] read(final int len);

    byte get(final long index);
    byte[] get(final long index, final int length);

    void write(final byte b);
    void write(final byte[] data, final int off, final int len);

    void set(final byte b, final long index);
    void set(final byte[] b, final long index, final int off, final int len);

    void copyFrom(final IOAccessible accessible, final long index, final long offset, final int length);
    void copyFrom(final IOAccessible accessible, final long offset, final int length);
    void copyFrom(final IOAccessible accessible, final int length);

}

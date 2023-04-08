package com.bethibande.memory;

import sun.misc.Unsafe;

import java.util.ArrayList;
import java.util.List;

/**
 * This is a class used for unsafe memory access.
 * Only use this if you truly know what you are doing.
 * This does not merely encapsulates the sun.misc.Unsafe class and does not contain <b>any</b>
 * safety checks.
 * Incorrect use of this class, will corrupt memory and or crash the jvm.
 */
final class UnsafeIOAccess extends IOAccess implements AutoCloseable {

    private final List<UnsafeIOAccess> slices = new ArrayList<>();
    
    private static final byte ZERO = 0x00;
    private static final long ADDRESS_FREED = -1L;
    private static final long BYTE_ARRAY_BASE_OFFSET;

    private static final Unsafe UNSAFE;

    static {
        try {
            UNSAFE = UnsafeHelper.getUnsafe();
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }

        BYTE_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);
    }

    public static UnsafeIOAccess allocate(final long size) {
        final long address = UNSAFE.allocateMemory(size);

        return new UnsafeIOAccess(address, size);
    }

    private long address;
    private final long size;

    public UnsafeIOAccess(final long address, final long size) {
        super(0L, size, true, true, true, null);
        this.address = address;
        this.size = size;

        this.init();
    }

    /**
     * Internal method, initializes memory by filling the entire buffer with 0
     */
    private void init() {
        UNSAFE.setMemory(null, address, size, ZERO);
    }

    @Override
    @SuppressWarnings("deprecation")
    protected void finalize() {
        this.free();
    }

    public void free() {
        if(this.address == ADDRESS_FREED) return;

        for(UnsafeIOAccess slice : this.slices) {
            slice.freeSlice();
        }
        
        UNSAFE.freeMemory(this.address);
        this.address = ADDRESS_FREED;
    }

    /**
     * Used to free a slice, this doesn't free the memory, since the slices memory will be freed by the parent anyways
     */
    private void freeSlice() {
        if(this.address == ADDRESS_FREED) return;
        
        this.address = ADDRESS_FREED;
    }

    @Override
    public void close() {
        this.free();
    }

    @Override
    public void skip(final long bytes) {
        super.idx(bytes);
    }

    @Override
    public void flush() {
        throw new UnsupportedOperationException("Flushing not supported");
    }

    @Override
    public void release() {
        this.free();
    }

    @Override
    public void fill(final byte b) {
        this.fill(0, size, b);
    }

    @Override
    public void clear() {
        this.fill(0, size, ZERO);
    }

    @Override
    public boolean isReleased() {
        return this.address == ADDRESS_FREED;
    }

    public void copy(final long offset, final long length, final long destination) {
        UNSAFE.copyMemory(this.address + offset, destination, length);
    }

    public void fill(final long offset, final long length, final byte value) {
        UNSAFE.setMemory(this.address + offset, length, value);
    }

    @Override
    public IOAccess slice(final long index, final long length) {
        final UnsafeIOAccess slice = new UnsafeIOAccess(this.address + index, length);
        this.slices.add(slice);
        return slice;
    }

    @Override
    public void write(final byte[] data, final int offset, final int length) {
        this.copyFromHeap(data, offset, super.idx(length), length);
    }

    @Override
    public byte[] read(final int length) {
        final byte[] arr = new byte[length];
        this.copyToHeap(super.idx(length), arr, 0, length);
        return arr;
    }

    @Override
    public byte[] get(final long index, final int length) {
        final byte[] arr = new byte[length];
        this.copyToHeap(index, arr, 0, length);
        return arr;
    }

    @Override
    public void set(final byte[] data, final long index, final int off, final int len) {
        this.copyFromHeap(data, off, index, len);
    }

    @Override
    public byte read() {
        return get(super.idx(1));
    }

    @Override
    public short readUByte() {
        return IOHelper.byteToUByte(get(super.idx(1)));
    }

    @Override
    public short readShort() {
        return getShort(super.idx(2));
    }

    @Override
    public int readUShort() {
        return IOHelper.shortToUShort(getShort(super.idx(2)));
    }

    @Override
    public int readInt() {
        return getInt(super.idx(4));
    }

    @Override
    public long readUInt() {
        return IOHelper.intToUInt(getInt(super.idx(4)));
    }

    @Override
    public long readLong() {
        return getLong(super.idx(8));
    }

    @Override
    public float readFloat() {
        return getFloat(super.idx(4));
    }

    @Override
    public double readDouble() {
        return getDouble(super.idx(8));
    }

    @Override
    public boolean readBoolean() {
        return getBoolean(super.idx(1));
    }

    public char readChar() {
        return getChar(super.idx(2));
    }

    @Override
    public void write(final byte b) {
        set(b, super.idx(1));
    }

    @Override
    public void writeUByte(final short b) {
        set(IOHelper.uByteToByte(b), super.idx(1));
    }

    @Override
    public void writeShort(final short s) {
        setShort(s, super.idx(2));
    }

    @Override
    public void writeUShort(final int s) {
        setShort(IOHelper.uShortToShort(s), super.idx(2));
    }

    @Override
    public void writeInt(final int i) {
        setInt(i, super.idx(4));
    }

    @Override
    public void writeUInt(final long i) {
        setInt(IOHelper.uIntToInt(i), super.idx(4));
    }

    @Override
    public void writeLong(final long l) {
        setLong(l, super.idx(8));
    }

    @Override
    public void writeFloat(final float f) {
        setFloat(f, super.idx(4));
    }

    @Override
    public void writeDouble(final double d) {
        setDouble(d, super.idx(8));
    }

    @Override
    public void writeBoolean(final boolean b) {
        setBoolean(b, super.idx(1));
    }

    public void writeChar(final char c) {
        setChar(c, super.idx(2));
    }

    @Override
    public void set(final byte b, final long offset) {
        UNSAFE.putByte(null, this.address + offset, b);
    }

    @Override
    public void setUByte(final short b, final long index) {
        set(IOHelper.uByteToByte(b), index);
    }

    @Override
    public void setShort(final short s, final long offset) {
        UNSAFE.putShort(null, this.address + offset, s);
    }

    @Override
    public void setUShort(final int s, final long index) {
        setShort(IOHelper.uShortToShort(s), index);
    }

    @Override
    public void setInt(final int i, final long offset) {
        UNSAFE.putInt(null, this.address + offset, i);
    }

    @Override
    public void setUInt(final long i, final long index) {
        setInt(IOHelper.uIntToInt(i), index);
    }

    @Override
    public void setLong(final long l, final long offset) {
        UNSAFE.putLong(null, this.address + offset, l);
    }

    @Override
    public void setFloat(final float f, final long offset) {
        UNSAFE.putFloat(null, this.address + offset, f);
    }

    @Override
    public void setDouble(final double d, final long offset) {
        UNSAFE.putDouble(null, this.address + offset, d);
    }

    @Override
    public void setBoolean(final boolean b, final long offset) {
        UNSAFE.putBoolean(null, this.address + offset, b);
    }

    public void setChar(final char c, final long offset) {
        UNSAFE.putChar(null, this.address + offset, c);
    }

    @Override
    public byte get(final long offset) {
        return UNSAFE.getByte(null, this.address + offset);
    }

    @Override
    public short getUByte(final long index) {
        return IOHelper.byteToUByte(get(index));
    }

    @Override
    public short getShort(final long offset) {
        return UNSAFE.getShort(null, this.address + offset);
    }

    @Override
    public int getUShort(final long index) {
        return IOHelper.shortToUShort(getShort(index));
    }

    @Override
    public int getInt(final long offset) {
        return UNSAFE.getInt(null, this.address + offset);
    }

    @Override
    public long getUInt(final long index) {
        return IOHelper.intToUInt(getInt(index));
    }

    @Override
    public long getLong(final long offset) {
        return UNSAFE.getLong(null, this.address + offset);
    }

    @Override
    public float getFloat(final long offset) {
        return UNSAFE.getFloat(null, this.address + offset);
    }

    @Override
    public double getDouble(final long offset) {
        return UNSAFE.getDouble(null, this.address + offset);
    }

    @Override
    public boolean getBoolean(final long offset) {
        return UNSAFE.getBoolean(null, this.address + offset);
    }

    public char getChar(final long offset) {
        return UNSAFE.getChar(null, this.address + offset);
    }

    @Override
    public void copyFrom(final IOAccess access, final long index, final long offset, final int length) {
        this.copyFromHeap(access.get(offset, length), 0, index, length);
    }

    /**
     * Copy bytes from heap to off-heap memory
     * @param arr the on-heap array to copy from
     * @param off offset within the array
     * @param dest offset within the native memory buffer
     * @param size the amount of bytes to copy starting at the given offset
     */
    public void copyFromHeap(final byte[] arr, final long off, final long dest, final long size) {
        UNSAFE.copyMemory(arr, BYTE_ARRAY_BASE_OFFSET + off, null, this.address + dest, size);
    }

    /**
     * Copy bytes from the native memory buffer to an on-heap array
     * @param src offset within the native memory buffer
     * @param arr the array to copy to
     * @param off the offset within the given array to put the copied data
     * @param size the amount of bytes to copy
     */
    public void copyToHeap(final long src, final byte[] arr, final long off, final int size) {
        UNSAFE.copyMemory(null, this.address + src, arr, BYTE_ARRAY_BASE_OFFSET + off, size);
    }

}

package com.bethibande.memory;

import jdk.incubator.foreign.MemoryLayout;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.UUID;

/**
 * Reads or writes data, to buffers, streams and more
 */
@SuppressWarnings("unused")
public class IOAccess {

    public static final byte ZERO = 0;
    public static final byte ONE = 1;

    /**
     * Only supports bulk read and write operations, all methods reading/writing only a single byte like {@link #read()}
     * are not supported by the resulting IOAccess.
     * Beware, this allocates off-heap memory, only use to allocate and manage large blocks of data.
     * The resulting access, will be owned by the Thread that invoke this method.
     * The access may not be accessed by any other thread
     * @param layout the memory layout used to map the underlying memory segment.
     */
    public static IOAccess scopedAlignedMemory(final MemoryLayout layout) {
        return new IOAccess(
                0,
                layout.byteSize(),
                true,
                true,
                true,
                IOScopedMemory.allocateAlignedNative(layout),
                true
        );
    }

    /**
     * Only supports bulk read and write operations, all methods reading/writing only a single byte like {@link #read()}
     * are not supported by the resulting IOAccess.
     * Beware, this allocates off-heap memory, only use to allocate and manage large blocks of data.
     * The resulting access, will be owned by the Thread that invoke this method.
     * The access may not be accessed by any other thread
     * @param size the byte size
     * @param alignment the memory alignment
     */
    public static IOAccess scopedAlignedMemory(final long size, final long alignment) {
        return new IOAccess(
                0,
                size,
                true,
                true,
                true,
                IOScopedMemory.allocateAlignedNative(size, alignment),
                true
        );
    }

    /**
     * Only supports bulk read and write operations, all methods reading/writing only a single byte like {@link #read()}
     * are not supported by the resulting IOAccess.
     * Beware, this allocates off-heap memory, only use to allocate and manage large blocks of data
     * The resulting access, will be owned by the Thread that invoke this method.
     * The access may not be accessed by any other thread
     * @param size the byte size
     */
    public static IOAccess scopedMemory(final long size) {
        return new IOAccess(
                0,
                size,
                true,
                true,
                true,
                IOScopedMemory.allocateNative(size),
                true
        );
    }

    /**
     * Creates a file backed buffer with read and write access
     * @param file the file backing the buffer
     */
    public static IOAccess randomAccess(final File file) {
        return randomAccess(file, true, true);
    }

    /**
     * Creates a file backed buffer with the given access
     * @param file the file backing the buffer
     * @param read grants read access
     * @param write grants write access
     */
    public static IOAccess randomAccess(final File file, final boolean write, final boolean read) {
        return new IOAccess(0, -1, true, write, read, new IOFile(file, write, read));
    }

    /**
     * Creates a stream backed access
     */
    public static IOAccess stream(final OutputStream out) {
        return stream(out, -1);
    }

    /**
     * Creates a stream backed access, with the given length
     */
    public static IOAccess stream(final OutputStream out, final long length) {
        return new IOAccess(0, length, false, true, false, new IOStream(out, null));
    }

    /**
     * Creates a stream backed access
     */
    public static IOAccess stream(final InputStream in) {
        return stream(in, -1);
    }

    /**
     * Creates a stream backed access, with the given length
     */
    public static IOAccess stream(final InputStream in, final long length) {
        return new IOAccess(0, length, false, false, true, new IOStream(null, in));
    }

    /**
     * Creates a stream backed access
     */
    public static IOAccess stream(final InputStream in, final OutputStream out) {
        return new IOAccess(
                0,
                -1,
                false,
                in != null,
                out != null,
                new IOStream(out, in)
        );
    }

    /**
     * Creates a byte array backed access
     * @param size the size of the byte array
     */
    public static IOAccess allocate(final int size) {
        return allocate(size, true, true, true);
    }

    protected static IOAccess from(final IOAccess context, final IOAccessible wrap, final long length) {
        final IOAccess value = new IOAccess(
                0,
                length,
                context.isIndexed(),
                context.canWrite(),
                context.canRead(),
                wrap
        );

        if(context.isOwned()) {
            value.setOwner(context.getOwner());
        }

        return value;
    }

    public static IOAccess allocate(final int size,
                                    final boolean isIndexed,
                                    final boolean canRead,
                                    final boolean canWrite) {
        return new IOAccess(0, size, isIndexed, canRead, canWrite, new IOBuffer(new byte[size]));
    }

    private long index;
    private final long length;
    private final boolean isIndexed;
    private final boolean canWrite;
    private final boolean canRead;
    private final IOAccessible accessible;

    private boolean released = false;

    private Long owner = null;

    protected IOAccess(final long index,
                       final long length,
                       final boolean isIndexed,
                       final boolean canWrite,
                       final boolean canRead,
                       final IOAccessible accessible) {
        this(index, length, isIndexed, canWrite, canRead, accessible, false);
    }

    protected IOAccess(final long index,
                       final long length,
                       final boolean isIndexed,
                       final boolean canWrite,
                       final boolean canRead,
                       final IOAccessible accessible,
                       final boolean setOwnership) {
        this.index = index;
        this.length = length;
        this.isIndexed = isIndexed;
        this.canWrite = canWrite;
        this.canRead = canRead;
        this.accessible = accessible;
        if(setOwnership) setOwner(Thread.currentThread());
    }

    public void acquireOwnership() {
        setOwner(Thread.currentThread());
    }

    protected void checkSlicing() {
        if(accessible().canSlice()) return;
        throw new IllegalAccessError("The underlying access doesn't permit slicing.");
    }

    protected void checkOwnership() {
        if(owner == null) return;
        if(owner != Thread.currentThread().getId()) throw new IllegalStateException("IOAccess is owned by another thread");
    }

    /**
     * !! Note: if the given thread-id doesn't exist,
     * or you loose access to the owner thread, the accessible may never be used again.<br>
     * <br>
     * The owning thread may transfer its ownership to another thread.<br>
     * Set the value to null to remove all ownership restrictions, note that
     * in some cases this will lead to exceptions if the underlying access does not
     * permit access from another thread.
     *
     * @param threadId the id of the new owner thread
     */
    public void setOwner(final Long threadId) {
        checkOwnership();
        this.owner = threadId;
    }

    /**
     * The owning thread may transfer its ownership to another thread.<br>
     * set the value to null to remove all ownership restrictions, note that
     * in some cases this will lead to exceptions if the underlying access does not
     * permit access from another thread.
     *
     * @param thread the new owner
     */
    public void setOwner(final Thread thread) {
        this.setOwner(thread.getId());
    }

    protected void checkIndexed() {
        if(!isIndexed) throw new UnsupportedOperationException("IOAccess does not support indexed read/write operations");
    }

    protected void checkWrite() {
        checkOwnership();
        if(released) throw new IllegalStateException("IOAccess has already been released.");
        if(!canWrite) throw new IllegalAccessError("Scope has no write access.");

    }

    protected void checkRead() {
        checkOwnership();
        if(released) throw new IllegalStateException("IOAccess has already been released.");
        if(!canRead) throw new IllegalAccessError("Scope has no read access.");
    }

    protected void checkWriteIndex(final long offset) {
        if(offset < 0) throw new UnsupportedOperationException("Cannot write a negative amount of bytes.");
        if(length < 0) return;
        if(index + offset > length) throw new IndexOutOfBoundsException(index + offset);
    }

    protected void checkWriteIndex(final long index, final long offset) {
        if(offset < 0) throw new UnsupportedOperationException("Cannot write a negative amount of bytes.");
        if(length < 0) return;
        if(index + offset > length) throw new IndexOutOfBoundsException(index + offset);
    }

    protected void checkReadIndex(final long offset) {
        if(offset < 0) throw new UnsupportedOperationException("Cannot read a negative amount of bytes.");
        if(length < 0) return;
        if(index + offset > length) throw new IndexOutOfBoundsException(index + offset);
    }

    protected void checkReadIndex(final long index, final long offset) {
        if(offset < 0) throw new UnsupportedOperationException("Cannot read a negative amount of bytes.");
        if(length < 0) return;
        if(index + offset > length) throw new IndexOutOfBoundsException(index + offset);
    }

    protected void index(final int offset) {
        if(length < 0) return;
        this.index += offset;
    }

    public byte read() {
        checkRead();
        checkReadIndex(1);
        index(-1);

        return accessible.read();
    }

    public short readUByte() {
        return IOHelper.uByteToShort(read());
    }

    public byte[] read(final int length) {
        checkRead();
        checkReadIndex(length);
        index(-length);

        return accessible.read(length);
    }

    public byte get(final long index) {
        checkRead();
        checkIndexed();
        checkReadIndex(index, 1);

        return accessible.get(index);
    }

    public short getUByte(final long index) {
        return IOHelper.uByteToShort(get(index));
    }

    public byte[] get(final long index, final int length) {
        checkRead();
        checkIndexed();
        checkReadIndex(index, length);

        return accessible.get(index, length);
    }

    public void write(final byte b) {
        checkWrite();
        checkWriteIndex(b);
        index(1);

        accessible.write(b);
    }

    public void writeUByte(final short s) {
        write(IOHelper.shortToUByte(s));
    }

    public void write(final byte[] data) {
        write(data, 0, data.length);
    }

    public void write(final byte[] data, final int offset, final int length) {
        checkWrite();
        checkWriteIndex(length);
        index(length);

        accessible.write(data, offset, length);
    }

    public void set(final byte[] data, final long index) {
        set(data, index, 0, data.length);
    }

    public void set(final byte[] data, final long index, final int off, final int len) {
        checkWrite();
        checkIndexed();
        checkWriteIndex(index, len);

        accessible.set(data, index, off, len);
    }

    public void set(final byte b, final long index) {
        checkWrite();
        checkIndexed();
        checkWriteIndex(index, 1);

        accessible.set(b, index);
    }

    public void setUByte(final short s, final long index) {
        set(IOHelper.shortToUByte(s), index);
    }

    public short readShort() {
        return IOHelper.bytesToShort(read(2));
    }

    public int readUShort() {
        return IOHelper.uShortToInt(IOHelper.bytesToShort(read(2)));
    }

    public void writeShort(final short s) {
        write(IOHelper.shortToBytes(s), 0, 2);
    }

    public void writeUShort(final int i) {
        write(IOHelper.shortToBytes(IOHelper.intToUShort(i)), 0, 2);
    }

    public short getShort(final long index) {
        return IOHelper.bytesToShort(get(index, 2));
    }

    public int getUShort(final long index) {
        return IOHelper.uShortToInt(IOHelper.bytesToShort(get(index, 2)));
    }

    public void setShort(final short s, final long index) {
        set(IOHelper.shortToBytes(s), index, 0, 2);
    }

    public void setUShort(final int s, final long index) {
        set(IOHelper.shortToBytes(IOHelper.intToUShort(s)), index, 0, 2);
    }


    public int readInt() {
        return IOHelper.bytesToInt(read(4));
    }

    public long readUInt() {
        return IOHelper.uIntToLong(IOHelper.bytesToInt(read(4)));
    }

    public void writeInt(final int i) {
        write(IOHelper.intToBytes(i), 0, 4);
    }

    public void writeUInt(final long i) {
        write(IOHelper.intToBytes(IOHelper.longToUInt(i)), 0, 4);
    }

    public int getInt(final long index) {
        return IOHelper.bytesToInt(get(index, 2));
    }

    public long getUInt(final long index) {
        return IOHelper.uIntToLong(IOHelper.bytesToInt(get(index, 4)));
    }

    public void setInt(final int i, final long index) {
        set(IOHelper.intToBytes(i), index, 0, 4);
    }

    public void setUInt(final long i, final long index) {
        set(IOHelper.intToBytes(IOHelper.longToUInt(i)), index, 0, 4);
    }

    public long readLong() {
        return IOHelper.bytesToLong(read(8));
    }

    public long getLong(final long index) {
        return IOHelper.bytesToLong(get(index, 8));
    }

    public void writeLong(final long l) {
        write(IOHelper.longToBytes(l), 0, 8);
    }

    public void setLong(final long l, final long index) {
        set(IOHelper.longToBytes(l), index, 0, 8);
    }


    public void writeFloat(final float f) {
        write(IOHelper.floatToBytes(f), 0, 4);
    }

    public float readFloat() {
        return IOHelper.bytesToFloat(read(4));
    }

    public void setFloat(final float f, final long index) {
        set(IOHelper.floatToBytes(f), index, 0, 4);
    }

    public float getFloat(final long index) {
        return IOHelper.bytesToFloat(get(index, 4));
    }

    public void writeDouble(final double d) {
        write(IOHelper.doubleToBytes(d), 0, 8);
    }

    public double readDouble() {
        return IOHelper.bytesToDouble(read(8));
    }

    public double getDouble(final long index) {
        return IOHelper.bytesToDouble(get(index, 8));
    }

    public void setDouble(final double d, final long index) {
        set(IOHelper.doubleToBytes(d), index, 0, 8);
    }

    public void writeBoolean(final boolean b) {
        write(b ? ONE : ZERO);
    }

    public boolean readBoolean() {
        return read() == ONE;
    }

    public void setBoolean(final boolean b, final long index) {
        set(b ? ONE: ZERO, index);
    }

    public boolean getBoolean(final long index) {
        return get(index) == ONE;
    }

    public void writeString(final String str, final Charset charset) {
        write(str.getBytes(charset));
    }

    public void setString(final String str, final Charset charset, final long index) {
        set(str.getBytes(charset), index);
    }

    public String readString(final int length, final Charset charset) {
        return new String(read(length), charset);
    }

    public String getString(final int length, final Charset charset, final long index) {
        return new String(get(index, length), charset);
    }

    public void writeStringByte(final String str, final Charset charset) {
        final byte[] bytes = str.getBytes(charset);
        if(bytes.length > 256) throw new IndexOutOfBoundsException("String too long, must be 256 bytes.");
        writeUByte((short) bytes.length);
        write(bytes);
    }

    public void setStringByte(final String str, final Charset charset, final long index) {
        final byte[] bytes = str.getBytes(charset);
        if(bytes.length > 256) throw new IndexOutOfBoundsException("String too long, must be 256 bytes.");
        setUByte((short) bytes.length, index);
        set(bytes, index + 2);
    }

    public String readStringByte(final Charset charset) {
        final short length = readUByte();
        final byte[] data = read(length);
        return new String(data, charset);
    }

    public String getStringByte(final Charset charset, final long index) {
        final short length = getUByte(index);
        final byte[] data = get(index, length);
        return new String(data, charset);
    }

    public void writeStringShort(final String str, final Charset charset) {
        final byte[] bytes = str.getBytes(charset);
        if(bytes.length > Short.MAX_VALUE*2) throw new IndexOutOfBoundsException("String too long, must be %d bytes.".formatted(Short.MAX_VALUE*2));
        writeUShort(bytes.length);
        write(bytes);
    }

    public void setStringShort(final String str, final Charset charset, final long index) {
        final byte[] bytes = str.getBytes(charset);
        if(bytes.length > 256) throw new IndexOutOfBoundsException("String too long, must be 256 bytes.");
        setUShort(bytes.length, index);
        set(bytes, index + 2);
    }

    public String readStringShort(final Charset charset) {
        final int length = readUShort();
        final byte[] data = read(length);
        return new String(data, charset);
    }

    public String getStringShort(final Charset charset, final long index) {
        final int length = getUShort(index);
        final byte[] data = get(index, length);
        return new String(data, charset);
    }

    public void writeUUID(final UUID id) {
        writeLong(id.getMostSignificantBits());
        writeLong(id.getLeastSignificantBits());
    }

    public UUID readUUID() {
        return new UUID(
                readLong(),
                readLong()
        );
    }

    public void setUUID(final UUID id, final long index) {
        setLong(id.getMostSignificantBits(), index);
        setLong(id.getLeastSignificantBits(), index + 8);
    }

    public UUID getUUID(final long index) {
        return new UUID(
                getLong(index),
                getLong(index + 8)
        );
    }


    public void copyFrom(final IOAccess access, final long index, final long offset, final int length) {
        checkWrite();
        checkWriteIndex(index, length);
        checkIndexed();
        accessible.copyFrom(access.accessible, index, offset, length);
    }

    public void copyFrom(final IOAccess access, final long offset, final int length) {
        checkWrite();
        checkWriteIndex(index, length);
        accessible.copyFrom(access.accessible, offset, length);
    }

    public void copyFrom(final IOAccess access, final int length) {
        checkWrite();
        checkWriteIndex(index, length);
        accessible.copyFrom(access.accessible, length);
    }

    /**
     * Slices the IOAccess, the new slice copies the current IOAccess's settings, e.g. is the IOAccess indexed,
     * read/write permissions and the ownership.<br>
     * The slice starts at the current index.
     * @return the new slice
     */
    public IOAccess slice(final long length) {
        return slice(index, length);
    }

    /**
     * Slices the IOAccess, the new slice copies the current IOAccess's settings, e.g. is the IOAccess indexed,
     * read/write permissions and the ownership.
     * @return the new slice
     */
    public IOAccess slice(final long index, final long length) {
        checkSlicing();
        checkReadIndex(index, length);

        return IOAccess.from(this, accessible.slice(index, length), length);
    }

    public void skip(final long bytes) {
        checkRead();
        checkReadIndex((int)bytes);
        accessible.skip(bytes);
    }

    public void flush() {
        checkOwnership();
        accessible.flush();
    }

    public void release() {
        checkOwnership();
        accessible.release();
        released = true;
    }

    /**
     * Fills the access with 0, starts from the current index.
     * The new index will be the end of the access.
     */
    public void fill(final byte b) {
        checkWrite();
        checkIndexed();

        final byte[] buffer = new byte[(int) Math.min(Integer.MAX_VALUE, remaining())];
        Arrays.fill(buffer, b);
        while(remaining() > 0) {
            final int write = (int) Math.min(Integer.MAX_VALUE, remaining());
            write(buffer, 0, write);
        }
    }

    /**
     * Fills the access with 0 and sets the read/write index to 0.
     */
    public void clear() {
        checkWrite();
        checkIndexed();

        flip();
        fill(ZERO);

        flip();
    }

    //-------------------------------------------------------------------------------------------
    // Getters and setters
    //-------------------------------------------------------------------------------------------

    public void flip() {
        checkIndexed();
        setIndex(0);
    }

    /**
     * @return the remaining bytes to be read or  written, or -1 if the access has no specified length
     */
    public long remaining() {
        return length - index;
    }

    public void setIndex(final long index) {
        checkIndexed();
        checkOwnership();
        this.index = index;
        this.accessible.setIndex(index);
    }

    /**
     * Checks if whether the access is owned by a thread
     */
    public boolean isOwned() {
        return owner != null;
    }

    public @Nullable Long getOwner() {
        return owner;
    }

    public boolean isReleased() {
        return released;
    }

    public long index() {
        return index;
    }

    public long length() {
        return length;
    }

    public boolean isIndexed() {
        return isIndexed;
    }

    public boolean canWrite() {
        return canWrite;
    }

    public boolean canRead() {
        return canRead;
    }

    public IOAccessible accessible() {
        return accessible;
    }
}

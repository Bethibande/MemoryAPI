package com.bethibande.memory;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import sun.misc.Unsafe;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.UUID;

/**
 * Reads or writes data, to buffers, streams and more
 */
@SuppressWarnings("unused")
public sealed class IOAccess permits NativeIOAccess, UnsafeIOAccess {

    private static final byte ZERO = 0;
    private static final byte ONE = 1;

    private static final Unsafe UNSAFE;
    /**
     * Byte offset of the index field within the class in memory
     */
    private static final long FIELD_INDEX_OFFSET;

    static {
        try {
            UNSAFE = UnsafeHelper.getUnsafe();
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }

        try {
            FIELD_INDEX_OFFSET = UNSAFE.objectFieldOffset(IOAccess.class.getDeclaredField("index"));
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * An IOAccess with incredible read/write speed, especially using set and get methods.
     * Allocates native, off-heap memory. <br>
     * Using this unsafe buffer is very dangerous and only recommended for experienced people. <br>
     * <b>!! Note:</b> There are no index checks when trying to access the resulting IOAccess.
     *          There are also no checks preventing you from reading/writing after freeing the IOAccess.
     *          The IOAccess will be freed, when calling {@link UnsafeIOAccess#free()} or {@link UnsafeIOAccess#close()}
     *          or during finalization. <br>
     * <b>!! Thread-safety:</b> This IOAccess implementation was not made with considerations to thread-safety.
     *                   Use multi-threading at your own risk.
     * @param size the mount of bytes to allocate
     * @return a new, unsafe, IOAccess
     */
    public static IOAccess unsafe(final long size) {
        return UnsafeIOAccess.allocate(size);
    }

    /**
     * Maps the given file into memory, the given path must be a file.
     * All changes made to the mapped file in memory, will be written to the underlying file.
     * Reading/Writing from the underlying file is handled by the operating system and
     * will not be specified here. <br>
     * For more detailed documentation look <a href="https://docs.oracle.com/en/java/javase/17/docs/api/jdk.incubator.foreign/jdk/incubator/foreign/MemorySegment.html#mapFile(java.nio.file.Path,long,long,java.nio.channels.FileChannel.MapMode,jdk.incubator.foreign.ResourceScope)">here</a>
     * @param file
     * @param offset offset within the file
     * @param size the amounts of bytes to map from the file offset
     * @param mode mode used to map memory
     * @return a new IOAccess that can be used to read/write the file in memory
     * @throws IOException
     */
    public static IOAccess map(final Path file,
                               final long offset,
                               final long size,
                               final FileChannel.MapMode mode) throws IOException {
        return new NativeIOAccess(
                0,
                size,
                true,
                mode == FileChannel.MapMode.PRIVATE || mode == FileChannel.MapMode.READ_WRITE,
                true,
                IOScopedMemory.mapFile(file, offset, size, mode),
                true
        );
    }

    /**
     * This method will create an access reading/writing at the given memory address. <br>
     * !! Note: Do not use this unless you know what you are doing, using this may cause page faults or corrupt memory.
     * @param address memory address to read/write from
     * @param size the size of the resulting access in bytes
     * @return a new access reading/writing from the given memory address
     */
    public static IOAccess atAddress(final long address, final long size) {
        return new NativeIOAccess(
                0,
                size,
                true,
                true,
                true,
                IOScopedMemory.atAddress(address, size),
                true
        );
    }

    /**
     * Beware, this allocates off-heap memory, only use to allocate and manage large blocks of data.
     * The resulting access, will be owned by the Thread that invoke this method.
     * The access may not be accessed by any other thread
     * @param memoryLayout the memory layout used to map the underlying memory segment.
     *                     Must be an instance of jdk.incubator.foreign.MemoryLayout.
     * @param byteSize the byte size of the memory layout
     */
    public static IOAccess scopedAlignedMemory(final Object memoryLayout, final long byteSize) {
        return new NativeIOAccess(
                0,
                byteSize,
                true,
                true,
                true,
                IOScopedMemory.allocateAlignedNative(memoryLayout),
                true
        );
    }

    /**
     * Beware, this allocates off-heap memory, only use to allocate and manage large blocks of data.
     * The resulting access, will be owned by the Thread that invoke this method.
     * The access may not be accessed by any other thread
     * @param size the byte size
     * @param alignment the memory alignment
     */
    public static IOAccess scopedAlignedMemory(final long size, final long alignment) {
        return new NativeIOAccess(
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
     * Beware, this allocates off-heap memory, only use to allocate and manage large blocks of data
     * The resulting access, will be owned by the Thread that invoke this method.
     * The access may not be accessed by any other thread
     * @param size the byte size
     */
    public static IOAccess scopedMemory(final long size) {
        return new NativeIOAccess(
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

    protected static IOAccess from(final IOAccess context, final IOScopedMemory wrap, final long length) {
        return new NativeIOAccess(
                0,
                length,
                context.isIndexed(),
                context.canWrite(),
                context.canRead(),
                wrap,
                true
        );
    }

    protected static IOAccess from(final IOAccess context, final IOAccessible wrap, final long length) {
        if(wrap instanceof IOScopedMemory mem) {
            return from(context, mem, length);
        }

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

    private volatile long index;
    private final long length;
    private final boolean isIndexed;
    private final boolean canWrite;
    private final boolean canRead;
    private final IOAccessible accessible;

    private boolean released = false;

    private ByteOrder order = ByteOrder.nativeOrder();

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

    protected void checkAvailable() {
        if(released) throw new IllegalStateException("The access has already been released");
    }

    protected void checkSlicing() {
        checkOwnership();
        if(released) throw new IllegalStateException("Cannot slice an access after it has been released.");
        if(!accessible().canSlice()) throw new IllegalAccessError("The underlying access doesn't permit slicing.");
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

    protected void checkOwnership() {
        if(owner == null) return;
        if(owner != Thread.currentThread().getId()) throw new IllegalStateException("IOAccess is owned by another thread");
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

    protected long idx(final long offset) {
        return UNSAFE.getAndAddLong(this, FIELD_INDEX_OFFSET, offset);
    }

    public byte read() {
        checkRead();
        checkReadIndex(1);
        idx(1);

        return accessible.read();
    }

    public short readUByte() {
        return IOHelper.byteToUByte(read());
    }

    public byte[] read(final int length) {
        checkRead();
        checkReadIndex(length);

        return accessible.get(idx(length), length);
    }

    public byte get(final long index) {
        checkRead();
        checkIndexed();
        checkReadIndex(index, 1);

        return accessible.get(index);
    }

    public short getUByte(final long index) {
        return IOHelper.byteToUByte(get(index));
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

        accessible.set(b, idx(1));
    }

    public void writeUByte(final short b) {
        write(IOHelper.uByteToByte(b));
    }

    public void write(final byte[] data) {
        write(data, 0, data.length);
    }

    public void write(final byte[] data, final int offset, final int length) {
        checkWrite();
        checkWriteIndex(length);

        accessible.set(data, idx(length), offset, length);
    }

    public void set(final byte[] data, final long index) {
        set(data, index, 0, data.length);
    }

    public void set(final byte[] data, final long index, final int off, final int len) {
        checkWrite();
        checkIndexed();
        //checkWriteIndex(index, len);

        accessible.set(data, index, off, len);
    }

    public void set(final byte b, final long index) {
        checkWrite();
        checkIndexed();
        //checkWriteIndex(index, 1);

        accessible.set(b, index);
    }

    public void setUByte(final short b, final long index) {
        set(IOHelper.uByteToByte(b), index);
    }

    public short readShort() {
        return IOHelper.bytesToShort(read(2), order);
    }

    public int readUShort() {
        return IOHelper.shortToUShort(IOHelper.bytesToShort(read(2), order));
    }

    public void writeShort(final short s) {
        write(IOHelper.shortToBytes(s, order), 0, 2);
    }

    public void writeUShort(final int s) {
        write(IOHelper.shortToBytes(IOHelper.uShortToShort(s), order), 0, 2);
    }

    public short getShort(final long index) {
        return IOHelper.bytesToShort(get(index, 2), order);
    }

    public int getUShort(final long index) {
        return IOHelper.shortToUShort(IOHelper.bytesToShort(get(index, 2), order));
    }

    public void setShort(final short s, final long index) {
        set(IOHelper.shortToBytes(s, order), index, 0, 2);
    }

    public void setUShort(final int s, final long index) {
        set(IOHelper.shortToBytes(IOHelper.uShortToShort(s), order), index, 0, 2);
    }


    public int readInt() {
        return IOHelper.bytesToInt(read(4), order);
    }

    public long readUInt() {
        return IOHelper.intToUInt(IOHelper.bytesToInt(read(4), order));
    }

    public void writeInt(final int i) {
        write(IOHelper.intToBytes(i, order), 0, 4);
    }

    public void writeUInt(final long i) {
        write(IOHelper.intToBytes(IOHelper.uIntToInt(i), order), 0, 4);
    }

    public int getInt(final long index) {
        return IOHelper.bytesToInt(get(index, 2), order);
    }

    public long getUInt(final long index) {
        return IOHelper.intToUInt(IOHelper.bytesToInt(get(index, 4), order));
    }

    public void setInt(final int i, final long index) {
        set(IOHelper.intToBytes(i, order), index, 0, 4);
    }

    public void setUInt(final long i, final long index) {
        set(IOHelper.intToBytes(IOHelper.uIntToInt(i), order), index, 0, 4);
    }

    public long readLong() {
        return IOHelper.bytesToLong(read(8), order);
    }

    public long getLong(final long index) {
        return IOHelper.bytesToLong(get(index, 8), order);
    }

    public void writeLong(final long l) {
        write(IOHelper.longToBytes(l, order), 0, 8);
    }

    public void setLong(final long l, final long index) {
        set(IOHelper.longToBytes(l, order), index, 0, 8);
    }


    public void writeFloat(final float f) {
        write(IOHelper.floatToBytes(f, order), 0, 4);
    }

    public float readFloat() {
        return IOHelper.bytesToFloat(read(4), order);
    }

    public void setFloat(final float f, final long index) {
        set(IOHelper.floatToBytes(f, order), index, 0, 4);
    }

    public float getFloat(final long index) {
        return IOHelper.bytesToFloat(get(index, 4), order);
    }

    public void writeDouble(final double d) {
        write(IOHelper.doubleToBytes(d, order), 0, 8);
    }

    public double readDouble() {
        return IOHelper.bytesToDouble(read(8), order);
    }

    public double getDouble(final long index) {
        return IOHelper.bytesToDouble(get(index, 8), order);
    }

    public void setDouble(final double d, final long index) {
        set(IOHelper.doubleToBytes(d, order), index, 0, 8);
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

    /**
     * Releases memory allocated by the access
     */
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

    /**
     * Returns the byte order used to read/write values, default value is {@link ByteOrder#nativeOrder()}
     */
    public ByteOrder getByteOrder() {
        return order;
    }

    /**
     * Set the byte order used to read/write values, default is {@link ByteOrder#nativeOrder()}
     */
    public void setByteOrder(final @NotNull ByteOrder order) {
        checkOwnership();
        checkAvailable();
        this.order = order;
        this.accessible.setByteOrder(order);
    }

    /**
     * Resets the read/write index
     */
    public void flip() {
        checkIndexed();
        checkOwnership();
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

    IOAccessible accessible() {
        return accessible;
    }
}

package com.bethibande.memory;

final class NativeIOAccess extends IOAccess {

    private final IOScopedMemory accessible;

    public NativeIOAccess(final long index,
                          final long length,
                          final boolean isIndexed,
                          final boolean canWrite,
                          final boolean canRead,
                          final IOScopedMemory accessible,
                          final boolean setOwnership) {
        super(index, length, isIndexed, canWrite, canRead, accessible, setOwnership);

        this.accessible = accessible;
    }

    @Override
    public byte read() {
        checkRead();
        checkReadIndex(1);
        return accessible.readByte();
    }

    @Override
    public short readUByte() {
        checkRead();
        checkReadIndex(1);
        return IOHelper.byteToUByte(accessible.readByte());
    }

    @Override
    public short readShort() {
        checkRead();
        checkReadIndex(2);
        return accessible.readShort();
    }

    @Override
    public int readUShort() {
        checkRead();
        checkReadIndex(2);
        return IOHelper.shortToUShort(accessible.readShort());
    }

    @Override
    public int readInt() {
        checkRead();
        checkReadIndex(4);
        return accessible.readInt();
    }

    @Override
    public long readUInt() {
        checkRead();
        checkReadIndex(4);
        return IOHelper.intToUInt(accessible.readInt());
    }

    @Override
    public long readLong() {
        checkRead();
        checkReadIndex(8);
        return accessible.readLong();
    }

    @Override
    public float readFloat() {
        checkRead();
        checkReadIndex(4);
        return accessible.readFloat();
    }

    @Override
    public double readDouble() {
        checkRead();
        checkReadIndex(8);
        return accessible.readDouble();
    }

    @Override
    public boolean readBoolean() {
        checkRead();
        checkReadIndex(1);
        return accessible.readBoolean();
    }

    @Override
    public void write(final byte b) {
        checkWrite();
        checkWriteIndex(1);
        accessible.writeByte(b);
    }

    @Override
    public void writeUByte(final short b) {
        checkWrite();
        checkWriteIndex(1);
        accessible.writeByte(IOHelper.uByteToByte(b));
    }

    @Override
    public void writeShort(final short s) {
        checkWrite();
        checkWriteIndex(2);
        accessible.writeShort(s);
    }

    @Override
    public void writeUShort(final int s) {
        checkWrite();
        checkWriteIndex(2);
        accessible.writeShort(IOHelper.uShortToShort(s));
    }

    @Override
    public void writeInt(final int i) {
        checkWrite();
        checkWriteIndex(4);
        accessible.writeInt(i);
    }

    @Override
    public void writeUInt(final long i) {
        checkWrite();
        checkWriteIndex(4);
        accessible.writeInt(IOHelper.uIntToInt(i));
    }

    @Override
    public void writeLong(final long l) {
        checkWrite();
        checkWriteIndex(8);
        accessible.writeLong(l);
    }

    @Override
    public void writeFloat(final float f) {
        checkWrite();
        checkWriteIndex(4);
        accessible.writeFloat(f);
    }

    @Override
    public void writeDouble(final double d) {
        checkWrite();
        checkWriteIndex(8);
        accessible.writeDouble(d);
    }

    @Override
    public void writeBoolean(final boolean b) {
        checkWrite();
        checkWriteIndex(1);
        accessible.writeBoolean(b);
    }

    @Override
    public void set(final byte b, final long index) {
        checkWrite();
        checkWriteIndex(index, 1);
        accessible.setByte(index, b);
    }

    @Override
    public void setUByte(final short b, final long index) {
        checkWrite();
        checkWriteIndex(index, 1);
        accessible.setByte(index, IOHelper.uByteToByte(b));
    }

    @Override
    public void setShort(final short s, final long index) {
        checkWrite();
        checkWriteIndex(index, 2);
        accessible.setShort(index, s);
    }

    @Override
    public void setUShort(final int s, final long index) {
        checkWrite();
        checkWriteIndex(index, 2);
        accessible.setShort(index, IOHelper.uShortToShort(s));
    }

    @Override
    public void setInt(final int i, final long index) {
        checkWrite();
        checkWriteIndex(index, 4);
        accessible.setInt(index, i);
    }

    @Override
    public void setUInt(final long i, final long index) {
        checkWrite();
        checkWriteIndex(index, 4);
        accessible.setInt(index, IOHelper.uIntToInt(i));
    }

    @Override
    public void setLong(final long l, final long index) {
        checkWrite();
        checkWriteIndex(index, 8);
        accessible.setLong(index, l);
    }

    @Override
    public void setFloat(final float f, final long index) {
        checkWrite();
        checkWriteIndex(index, 4);
        accessible.setFloat(index, f);
    }

    @Override
    public void setDouble(final double d, final long index) {
        checkWrite();
        checkWriteIndex(index, 8);
        accessible.setDouble(index, d);
    }

    @Override
    public void setBoolean(final boolean b, final long index) {
        checkWrite();
        checkWriteIndex(index, 1);
        accessible.setBoolean(index, b);
    }

    @Override
    public byte get(final long index) {
        checkRead();
        checkReadIndex(index, 1);
        return accessible.getByte(index);
    }

    @Override
    public short getUByte(final long index) {
        checkRead();
        checkReadIndex(index, 1);
        return IOHelper.byteToUByte(accessible.getByte(index));
    }

    @Override
    public short getShort(final long index) {
        checkRead();
        checkReadIndex(index, 2);
        return accessible.getShort(index);
    }

    @Override
    public int getUShort(final long index) {
        checkRead();
        checkReadIndex(index, 2);
        return IOHelper.shortToUShort(accessible.getShort(index));
    }

    @Override
    public int getInt(final long index) {
        checkRead();
        checkReadIndex(index, 4);
        return accessible.getInt(index);
    }

    @Override
    public long getUInt(final long index) {
        checkRead();
        checkReadIndex(index, 4);
        return IOHelper.intToUInt(accessible.getInt(index));
    }

    @Override
    public long getLong(final long index) {
        checkRead();
        checkReadIndex(index, 8);
        return accessible.getLong(index);
    }

    @Override
    public float getFloat(final long index) {
        checkRead();
        checkReadIndex(index, 4);
        return accessible.getFloat(index);
    }

    @Override
    public double getDouble(final long index) {
        checkRead();
        checkReadIndex(index, 8);
        return accessible.getDouble(index);
    }

    @Override
    public boolean getBoolean(final long index) {
        checkRead();
        checkReadIndex(index, 1);
        return accessible.getBoolean(index);
    }
}

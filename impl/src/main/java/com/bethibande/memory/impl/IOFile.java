package com.bethibande.memory.impl;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public class IOFile implements IOAccessible {

    private final RandomAccessFile raf;

    public IOFile(final File file, final boolean write, final boolean read) {
        try {
            this.raf = new RandomAccessFile(file,  (read ? "r": "") + (write ? "w": "") + "s");
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public IOFile(final RandomAccessFile raf) {
        this.raf = raf;
    }

    @Override
    public void setIndex(final long index) {
        try {
            raf.seek(index);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void skip(final long bytes) {
        try {
            raf.seek(raf.getFilePointer() + bytes);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void flush() {
        throw new UnsupportedOperationException("IOAccessible not flushable");
    }

    @Override
    public void release() {
        try {
            raf.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte read() {
        try {
            return raf.readByte();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] read(final int len) {
        final byte[] bytes = new byte[len];
        try {
            raf.read(bytes, 0, len);
            return bytes;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte get(final long index) {
        final long pointer;
        try {
            pointer = raf.getFilePointer();

            raf.seek(index);
            final byte b = raf.readByte();
            raf.seek(pointer);

            return b;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] get(final long index, final int length) {
        final long pointer;
        try {
            pointer = raf.getFilePointer();

            raf.seek(index);
            final byte[] bytes = new byte[length];
            raf.read(bytes, 0, length);
            raf.seek(pointer);

            return bytes;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void write(final byte b) {
        try {
            raf.writeByte(b);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void write(final byte[] data, final int off, final int len) {
        try {
            raf.write(data, off, len);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void set(final byte b, final long index) {
        try {
            final long pointer = raf.getFilePointer();

            raf.seek(index);
            raf.write(b);
            raf.seek(pointer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public void set(final byte[] b, final long index, final int off, final int len) {
        try {
            final long pointer = raf.getFilePointer();

            raf.seek(index);
            raf.write(b, off, len);
            raf.seek(pointer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void copyFrom(final IOAccessible accessible, final long index, final long offset, final int length) {
        final byte[] data = accessible.get(offset, length);
        set(data, index, 0, data.length);
    }

    @Override
    public void copyFrom(final IOAccessible accessible, final long offset, final int length) {
        final byte[] data = accessible.get(offset, length);
        write(data, 0, data.length);
    }

    @Override
    public void copyFrom(final IOAccessible accessible, final int length) {
        final byte[] data = accessible.read(length);
        write(data, 0, data.length);
    }
}
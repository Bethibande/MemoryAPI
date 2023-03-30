package com.bethibande.memory.impl;

import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class IOStream implements IOAccessible {

    private final OutputStream output;
    private final InputStream input;

    public IOStream(final @Nullable OutputStream output, final @Nullable InputStream input) {
        this.output = output;
        this.input = input;
    }

    @Override
    public void setIndex(final long index) {
        throw new UnsupportedOperationException("IOAccessible is not indexed.");
    }

    @Override
    public void skip(final long bytes) {
        if(input != null) {
            try {
                input.skipNBytes(bytes);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void flush() {
        if(output != null) {
            try {
                output.flush();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void release() {
        if(output != null) {
            try {
                output.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        if(input != null) {
            try {
                input.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void checkRead() {
        if(input == null) throw new IllegalAccessError("IOAccessible is not readable");
    }

    @Override
    public byte read() {
        checkRead();
        try {
            return input.readNBytes(1)[0];
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] read(final int len) {
        checkRead();
        try {
            return input.readNBytes(len);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte get(final long index) {
        throw new UnsupportedOperationException("IOAccessible is not indexed.");
    }

    @Override
    public byte[] get(final long index, final int length) {
        throw new UnsupportedOperationException("IOAccessible is not indexed.");
    }

    private void checkWrite() {
        if(output == null) throw new UnsupportedOperationException("IOAccessible is not writable.");
    }

    @Override
    public void write(final byte b) {
        checkWrite();
        try {
            output.write(b);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void write(final byte[] data, final int off, final int len) {
        checkWrite();
        try {
            output.write(data, off, len);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void set(final byte b, final long index) {
        throw new UnsupportedOperationException("IOAccessible is not indexed.");
    }

    @Override
    public void set(final byte[] b, final long index, final int off, final int len) {
        throw new UnsupportedOperationException("IOAccessible is not indexed.");
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
package com.bethibande.memory;

import java.nio.ByteOrder;

/**
 * There isn't much to see here, this class simply defines a bunch of helper methods
 */
public class IOHelper {

    public static byte uByteToByte(final short s) {
        return (byte) s;
    }

    public static short uShortToShort(final int i) {
        return (short)i;
    }

    public static int uIntToInt(final long l) {
        return (int)l;
    }

    public static short byteToUByte(final byte b) {
        return (short) (b & 0xff);
    }

    public static int shortToUShort(final short s) {
        return s & 0xffff;
    }

    public static long intToUInt(final int i) {
        return (long) i & 0xffffffffL;
    }

    public static byte[] shortToBytes(final short s, final ByteOrder order) {
        if(order == ByteOrder.BIG_ENDIAN) {
            return new byte[] {
                    (byte)(s >>> 8),
                    (byte)(s)
            };
        } else {
            return new byte[] {
                    (byte)(s),
                    (byte)(s >>> 8)
            };
        }
    }

    public static byte[] intToBytes(final int i, final ByteOrder order) {
        if(order == ByteOrder.BIG_ENDIAN) {
            return new byte[] {
                    (byte)(i >>> 24),
                    (byte)(i >>> 16),
                    (byte)(i >>> 8),
                    (byte)(i)
            };
        } else {
            return new byte[] {
                    (byte)(i),
                    (byte)(i >>> 8),
                    (byte)(i >>> 16),
                    (byte)(i >>> 24)
            };
        }
    }

    public static byte[] longToBytes(final long l, final ByteOrder order) {
        if(order == ByteOrder.BIG_ENDIAN) {
            return new byte[] {
                    (byte)(l >>> 56),
                    (byte)(l >>> 48),
                    (byte)(l >>> 40),
                    (byte)(l >>> 32),
                    (byte)(l >>> 24),
                    (byte)(l >>> 16),
                    (byte)(l >>> 8),
                    (byte)(l)
            };
        } else {
            return new byte[] {
                    (byte)(l),
                    (byte)(l >>> 8),
                    (byte)(l >>> 16),
                    (byte)(l >>> 24),
                    (byte)(l >>> 32),
                    (byte)(l >>> 40),
                    (byte)(l >>> 48),
                    (byte)(l >>> 56)
            };
        }
    }

    public static byte[] floatToBytes(final float f, final ByteOrder order) {
        final int i = Float.floatToRawIntBits(f);
        return intToBytes(i, order);
    }

    public static byte[] doubleToBytes(final double d, final ByteOrder order) {
        final long l = Double.doubleToRawLongBits(d);
        return longToBytes(l, order);
    }

    public static short bytesToShort(final byte[] bytes, final ByteOrder order) {
        if(order == ByteOrder.BIG_ENDIAN) {
            return (short) (((bytes[0] & 0xff) << 8) | (bytes[1] & 0xff));
        } else {
            return (short) ((bytes[0] & 0xff) | ((bytes[1] & 0xff) << 8));
        }
    }

    public static int bytesToInt(final byte[] bytes, final ByteOrder order) {
        if(order == ByteOrder.BIG_ENDIAN) {
            return (bytes[0] & 0xff) << 24
                    | (bytes[1] & 0xff) << 16
                    | (bytes[2] & 0xff) << 8
                    | (bytes[3] & 0xff);
        } else {
            return (bytes[0] & 0xff)
                    | (bytes[1] & 0xff) << 8
                    | (bytes[2] & 0xff) << 16
                    | (bytes[3] & 0xff) << 24;
        }
    }

    public static long bytesToLong(final byte[] bytes, final ByteOrder order) {
        if(order == ByteOrder.BIG_ENDIAN) {
            return (bytes[0] & 0xffL) << 56L
                    | (bytes[1] & 0xffL) << 48L
                    | (bytes[2] & 0xffL) << 40L
                    | (bytes[3] & 0xffL) << 32L
                    | (bytes[4] & 0xffL) << 24L
                    | (bytes[5] & 0xffL) << 16L
                    | (bytes[6] & 0xffL) << 8L
                    | (bytes[7] & 0xffL);
        } else {
            return (bytes[0] & 0xffL)
                    | (bytes[1] & 0xffL) << 8L
                    | (bytes[2] & 0xffL) << 16L
                    | (bytes[3] & 0xffL) << 24L
                    | (bytes[4] & 0xffL) << 32L
                    | (bytes[5] & 0xffL) << 40L
                    | (bytes[6] & 0xffL) << 48L
                    | (bytes[7] & 0xffL) << 56L;
        }
    }

    public static float bytesToFloat(final byte[] bytes, final ByteOrder order) {
        final int i = bytesToInt(bytes, order);
        return Float.intBitsToFloat(i);
    }

    public static double bytesToDouble(final byte[] bytes, final ByteOrder order) {
        final long l = bytesToLong(bytes, order);
        return Double.longBitsToDouble(l);
    }

}
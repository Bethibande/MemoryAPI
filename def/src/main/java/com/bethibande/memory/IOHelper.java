package com.bethibande.memory;

/**
 * There isn't much to see here, this class simply defines a bunch of helper methods
 */
public class IOHelper {

    public static byte shortToUByte(final short s) {
        return (byte) s;
    }

    public static short intToUShort(final int i) {
        return (short)i;
    }

    public static int longToUInt(final long l) {
        return (int)l;
    }

    public static short uByteToShort(final byte b) {
        return (short) (b & 0xff);
    }

    public static int uShortToInt(final short s) {
        return s & 0xffff;
    }

    public static long uIntToLong(final int i) {
        return (long) i & 0xffffffffL;
    }

    public static byte[] shortToBytes(final short s) {
        return new byte[] {
                (byte)(s >>> 8),
                (byte)(s)
        };
    }

    public static byte[] intToBytes(final int i) {
        return new byte[] {
                (byte)(i >>> 24),
                (byte)(i >>> 16),
                (byte)(i >>> 8),
                (byte)(i)
        };
    }

    public static byte[] longToBytes(final long l) {
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
    }

    public static byte[] floatToBytes(final float f) {
        final int i = Float.floatToRawIntBits(f);
        return intToBytes(i);
    }

    public static byte[] doubleToBytes(final double d) {
        final long l = Double.doubleToRawLongBits(d);
        return longToBytes(l);
    }

    public static short bytesToShort(final byte[] bytes) {
        return (short) (((bytes[0] & 0xff) << 8) | (bytes[1] & 0xff));
    }

    public static int bytesToInt(final byte[] bytes) {
        return (bytes[0] & 0xff) << 24
                | (bytes[1] & 0xff) << 16
                | (bytes[2] & 0xff) << 8
                | (bytes[3] & 0xff);
    }

    public static long bytesToLong(final byte[] bytes) {
        return (bytes[0] & 0xffL) << 56L
                | (bytes[1] & 0xffL) << 48L
                | (bytes[2] & 0xffL) << 40L
                | (bytes[3] & 0xffL) << 32L
                | (bytes[4] & 0xffL) << 24L
                | (bytes[5] & 0xffL) << 16L
                | (bytes[6] & 0xffL) << 8L
                | (bytes[7] & 0xffL);
    }

    public static float bytesToFloat(final byte[] bytes) {
        final int i = bytesToInt(bytes);
        return Float.intBitsToFloat(i);
    }

    public static double bytesToDouble(final byte[] bytes) {
        final long l = bytesToLong(bytes);
        return Double.longBitsToDouble(l);
    }

}
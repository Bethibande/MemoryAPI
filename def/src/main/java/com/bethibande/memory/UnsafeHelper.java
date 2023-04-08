package com.bethibande.memory;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

public class UnsafeHelper {

    public static Unsafe getUnsafe() throws NoSuchFieldException, IllegalAccessException {
        final Field f = Unsafe.class.getDeclaredField("theUnsafe");
        f.setAccessible(true);
        return (Unsafe) f.get(null);
    }

}

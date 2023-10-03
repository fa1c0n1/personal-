package com.apple.aml.stargate.common.utils;

import stormpot.Allocator;
import stormpot.BasePoolable;
import stormpot.Slot;

public final class ObjectPoolUtils {
    private ObjectPoolUtils() {
    }

    public static class GenericPoolable extends BasePoolable {
        public GenericPoolable(final Slot slot) {
            super(slot);
        }
    }

    public static class GenericAllocator implements Allocator<GenericPoolable> {
        public GenericPoolable allocate(final Slot slot) throws Exception {
            return new GenericPoolable(slot);
        }

        public void deallocate(final GenericPoolable poolable) throws Exception {
        }
    }
}

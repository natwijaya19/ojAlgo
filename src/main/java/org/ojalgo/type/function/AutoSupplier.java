/*
 * Copyright 1997-2022 Optimatika
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package org.ojalgo.type.function;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Utilities for {@link AutoCloseable} {@link Supplier}:s
 *
 * @author apete
 */
@FunctionalInterface
public interface AutoSupplier<T> extends AutoCloseable, Supplier<T>, AutoFunctional {

    static <T> AutoSupplier<T> empty() {
        return () -> null;
    }

    /**
     * Get something but map/transform before returning it
     */
    static <T, U> AutoSupplier<U> mapped(final Supplier<T> supplier, final Function<T, U> mapper) {
        return new MappedSupplier<>(supplier, mapper);
    }

    /**
     * Multiple suppliers supply to a queue, then you get from that queue. There will be 1 thread (executor
     * task) per supplier.
     */
    static <T> AutoSupplier<T> queued(final ExecutorService executor, final BlockingQueue<T> queue, final Supplier<T>... suppliers) {
        return new QueuedSupplier<>(executor, queue, suppliers);
    }

    /**
     * Get from all these suppliers in sequence, 1 at the time
     */
    static <T> AutoSupplier<T> sequenced(final Supplier<T>... suppliers) {
        return new SequencedSupplier<>(suppliers);
    }

    default void close() throws Exception {
        // Default implementation does nothing
    }

}

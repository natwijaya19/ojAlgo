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
package org.ojalgo.netio;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.function.IntSupplier;

import org.ojalgo.concurrent.DaemonPoolExecutor;
import org.ojalgo.concurrent.Parallelism;
import org.ojalgo.netio.ToFileWriter.Builder;

abstract class ReaderWriterBuilder<B extends ReaderWriterBuilder<B>> {

    private static volatile ExecutorService EXECUTOR = null;

    private static ExecutorService executor() {
        if (EXECUTOR == null) {
            synchronized (Builder.class) {
                if (EXECUTOR == null) {
                    EXECUTOR = DaemonPoolExecutor.newCachedThreadPool("ojAlgo IO");
                }
            }
        }
        return EXECUTOR;
    }

    private int myCapacity = 1024;
    private ExecutorService myExecutor = null;
    private final File[] myFiles;
    private IntSupplier myParallelism = Parallelism.CORES.limit(32);

    ReaderWriterBuilder(final File[] files) {
        super();
        myFiles = files;
    }

    public B capacity(final int capacity) {
        myCapacity = capacity;
        return (B) this;
    }

    public final B executor(final ExecutorService executor) {
        myExecutor = executor;
        return (B) this;
    }

    public B parallelism(final int parallelism) {
        return this.parallelism(() -> parallelism);
    }

    public B parallelism(final IntSupplier parallelism) {
        myParallelism = parallelism;
        return (B) this;
    }

    int getCapacity() {
        return myCapacity;
    }

    final ExecutorService getExecutor() {
        if (myExecutor == null) {
            myExecutor = ReaderWriterBuilder.executor();
        }
        return myExecutor;
    }

    File[] getFiles() {
        return myFiles;
    }

    int getParallelism() {
        return myParallelism.getAsInt();
    }
}

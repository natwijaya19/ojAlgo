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

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.Collection;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipInputStream;

import org.ojalgo.type.function.AutoSupplier;
import org.ojalgo.type.function.OperatorWithException;

public interface FromFileReader<T> extends AutoSupplier<T>, Closeable {

    public static final class Builder extends ReaderWriterBuilder<FromFileReader.Builder> {

        Builder(final File[] files) {
            super(files);
        }

        public <T> FromFileReader<T> build(final Function<File, AutoSupplier<T>> factory) {

            File[] files = this.getFiles();

            AutoSupplier<T>[] readers = (AutoSupplier<T>[]) new AutoSupplier<?>[files.length];
            for (int i = 0; i < readers.length; i++) {
                readers[i] = factory.apply(files[i]);
            }

            LinkedBlockingDeque<T> queue = new LinkedBlockingDeque<>(this.getCapacity());

            AutoSupplier<T> supplier;
            if (readers.length > 1 || this.getParallelism() == 1) {
                supplier = AutoSupplier.queued(this.getExecutor(), queue, AutoSupplier.sequenced(readers));
            } else {
                supplier = AutoSupplier.queued(this.getExecutor(), queue, readers);
            }

            return new WrappedSupplierReader<>(supplier);
        }

    }

    static <T extends Serializable> T deserializeObjectFromFile(final File file) {
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file))) {
            return (T) ois.readObject();
        } catch (IOException | ClassNotFoundException cause) {
            throw new RuntimeException(cause);
        }
    }

    static InputStream input(final File file) {

        try {

            NetioUtils.mkdirs(file.getParentFile());
            String name = file.getName();
            InputStream retVal = new FileInputStream(file);

            if (name.endsWith(".gz")) {
                retVal = new GZIPInputStream(retVal);
            } else if (name.endsWith(".zip")) {
                retVal = new ZipInputStream(retVal);
            }

            return retVal;

        } catch (IOException cause) {
            throw new RuntimeException(cause);
        }
    }

    static InputStream input(final File file, final OperatorWithException<InputStream> filter) {
        return filter.apply(FromFileReader.input(file));
    }

    static Builder of(final File... file) {
        return new Builder(file);
    }

    static Builder of(final ShardedFile shards) {
        return new Builder(shards.shards());
    }

    default void close() throws IOException {
        try {
            AutoSupplier.super.close();
        } catch (Exception cause) {
            if (cause instanceof IOException) {
                throw (IOException) cause;
            } else {
                throw new RuntimeException(cause);
            }
        }
    }

    default int drainTo(final Collection<? super T> batchContainer, final int maxElements) {

        int retVal = 0;

        T item = null;
        while (retVal < maxElements && (item = this.get()) != null) {
            batchContainer.add(item);
            retVal++;
        }

        return retVal;
    }

    default T get() {
        return this.read();
    }

    default void processAll(final Consumer<T> processor) {
        T next = null;
        while ((next = this.read()) != null) {
            processor.accept(next);
        }
    }

    T read();

}

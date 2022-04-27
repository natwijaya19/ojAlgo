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

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;

import org.ojalgo.type.function.ScoredPairConsumer;
import org.ojalgo.type.keyvalue.EntryPair;
import org.ojalgo.type.keyvalue.EntryPair.Dual;
import org.ojalgo.type.keyvalue.EntryPair.KeyedPrimitive;

public abstract class ScoredPairReader<T> implements FromFileReader<EntryPair.KeyedPrimitive<Dual<T>>> {

    static final class AggregateReader<T> extends ScoredPairReader<T> {

        private final ScoredPairReader<T> myLeftReader;
        private final ScoredPairReader<T> myRightReader;

        AggregateReader(final ScoredPairReader<T> leftReader, final ScoredPairReader<T> rightReader) {
            super();
            myLeftReader = leftReader;
            myRightReader = rightReader;
            this.max();
        }

        public void close() throws IOException {
            myLeftReader.close();
            myRightReader.close();
        }

        public KeyedPrimitive<Dual<T>> read() {

            KeyedPrimitive<Dual<T>> retVal = null;

            if (myLeftReader.nextSimilarity >= myRightReader.nextSimilarity) {
                retVal = myLeftReader.read();
            } else {
                retVal = myRightReader.read();
            }

            this.max();

            return retVal;
        }

        @Override
        public void read(final ScoredPairConsumer<T> consumer) {

            if (myLeftReader.nextSimilarity >= myRightReader.nextSimilarity) {
                myLeftReader.read(consumer);
            } else {
                myRightReader.read(consumer);
            }

            this.max();
        }

        private void max() {
            nextSimilarity = Math.max(myLeftReader.nextSimilarity, myRightReader.nextSimilarity);
        }

        @Override
        boolean hasNext(final float threshold) {
            return nextSimilarity > threshold;
        }

    }

    static final class DummyReader<T> extends ScoredPairReader<T> {

        DummyReader() {
            super();
        }

        public void close() throws IOException {

        }

        public KeyedPrimitive<Dual<T>> read() {
            return null;
        }

        @Override
        public void read(final ScoredPairConsumer consumer) {

        }

        @Override
        boolean hasNext(final float threshold) {
            return false;
        }

    }

    static final class SingleFileReader<T> extends ScoredPairReader<T> {

        private final DataInputStream myInput;
        private final DataInterpreter<T> myInterpreter = null;
        private T myNextId1 = null;
        private T myNextId2 = null;

        SingleFileReader(final File file) {

            super();

            DataInputStream tmpInput = null;
            try {
                tmpInput = new DataInputStream(new BufferedInputStream(new FileInputStream(file)));
            } catch (FileNotFoundException exception) {
                exception.printStackTrace();
            }
            myInput = tmpInput;
            this.doRead();
        }

        public void close() throws IOException {
            myInput.close();
        }

        public KeyedPrimitive<Dual<T>> read() {

            KeyedPrimitive<Dual<T>> retVal = null;

            if ((myNextId1 != null) && (myNextId2 != null)) {
                retVal = EntryPair.of(myNextId1, myNextId2, nextSimilarity);
                this.doRead();
            }

            return retVal;
        }

        @Override
        public void read(final ScoredPairConsumer<T> consumer) {
            consumer.accept(myNextId1, myNextId2, nextSimilarity);
            this.doRead();
        }

        private void doRead() {
            try {
                myNextId1 = myInterpreter.deserialize(myInput);
                myNextId2 = myInterpreter.deserialize(myInput);
                nextSimilarity = myInput.readFloat();
            } catch (IOException cause) {
                myNextId1 = null;
                myNextId2 = null;
                nextSimilarity = NULL;
            }
        }

        @Override
        boolean hasNext(final float threshold) {
            return (myNextId1 != null) && (myNextId2 != null) && (nextSimilarity > threshold);
        }

    }

    static final float NULL = -Float.MAX_VALUE;

    public static <T> ScoredPairReader<T> newInstance(final Collection<? extends File> files) {
        return ScoredPairReader.make(files.stream().toArray(File[]::new), 0, files.size());
    }

    public static <T> ScoredPairReader<T> newInstance(final File file) {
        return new ScoredPairReader.SingleFileReader(file);
    }

    public static <T> ScoredPairReader<T> newInstance(final File... files) {
        return ScoredPairReader.make(files, 0, files.length);
    }

    public static <T> ScoredPairReader<T> newInstance(final String pathToFile) {
        return new ScoredPairReader.SingleFileReader(new File(pathToFile));
    }

    private static <T> ScoredPairReader<T> make(final File[] files, final int first, final int limit) {
        int count = limit - first;
        if (count == 0) {
            return new ScoredPairReader.DummyReader();
        } else if (count == 1) {
            return new ScoredPairReader.SingleFileReader(files[first]);
        } else {
            int split = first + (count / 2);
            ScoredPairReader<T> leftReader = ScoredPairReader.make(files, first, split);
            ScoredPairReader<T> rightReader = ScoredPairReader.make(files, split, limit);
            return new ScoredPairReader.AggregateReader<>(leftReader, rightReader);
        }
    }

    float nextSimilarity = NULL;

    ScoredPairReader() {
        super();
    }

    public final boolean hasNext() {
        return this.hasNext(NULL);
    }

    public abstract void read(ScoredPairConsumer<T> consumer);

    public void processAll(final float similarityScoreThreshold, final ScoredPairConsumer<T> processor) {
        while (this.hasNext(similarityScoreThreshold)) {
            this.read(processor);
        }
    }

    public void processAll(final ScoredPairConsumer<T> processor) {
        this.processAll(NULL, processor);
    }

    abstract boolean hasNext(float threshold);

}

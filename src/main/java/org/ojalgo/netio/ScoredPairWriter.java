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

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collection;

import org.ojalgo.type.function.ScoredPairConsumer;
import org.ojalgo.type.keyvalue.EntryPair;
import org.ojalgo.type.keyvalue.EntryPair.Dual;
import org.ojalgo.type.keyvalue.EntryPair.KeyedPrimitive;

public class ScoredPairWriter<T> implements ToFileWriter<EntryPair.KeyedPrimitive<Dual<T>>>, ScoredPairConsumer<T> {

    public static <T> ScoredPairWriter<T> newInstance(final File file) {
        return new ScoredPairWriter<>(file, false);
    }

    public static <T> ScoredPairWriter<T> newInstance(final File file, final boolean append) {
        return new ScoredPairWriter<>(file, append);
    }

    public static <T> ScoredPairWriter<T> newInstance(final String filePath) {
        return new ScoredPairWriter<>(new File(filePath), false);
    }

    private BufferedWriter bw;
    private final DataInterpreter<T> myInterpreter = null;
    private final DataOutputStream myOutput;

    ScoredPairWriter(final File file, final boolean append) {

        super();

        DataOutputStream tmpOutput = null;
        try {
            tmpOutput = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file, append)));

        } catch (IOException e) {
            e.printStackTrace();
        }
        myOutput = tmpOutput;
    }

    public void accept(final T id1, final T id2, final float score) {
        try {
            myInterpreter.serialize(id1, myOutput);
            myInterpreter.serialize(id2, myOutput);
            myOutput.writeFloat(score);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {

        try {
            myOutput.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void write(final KeyedPrimitive<Dual<T>> pair) {
        this.accept(pair.getKey().first, pair.getKey().second, pair.floatValue());
    }

    public void write(final T id1, final T id2, final float score) {
        this.accept(id1, id2, score);
    }

    public void writeAll(final Collection<KeyedPrimitive<Dual<T>>> pairs) {
        for (KeyedPrimitive<Dual<T>> pair : pairs) {
            this.write(pair);
        }
    }

    public void accept(final KeyedPrimitive<Dual<T>> item) {
        ScoredPairConsumer.super.accept(item);
    }

}

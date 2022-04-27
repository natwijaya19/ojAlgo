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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.ojalgo.type.function.OperatorWithException;

public interface DataInterpreter<T> extends DataReader.Deserializer<T>, DataWriter.Serializer<T> {

    DataInterpreter<String> STRING = new DataInterpreter<String>() {

        public String deserialize(final DataInput input) throws IOException {
            return input.readUTF();
        }

        public void serialize(final String data, final DataOutput output) throws IOException {
            output.writeUTF(data);
        }

    };

    default DataReader<T> newReader(final File file) {
        return DataReader.of(file, this);
    }

    default DataReader<T> newReader(final File file, final OperatorWithException<InputStream> filter) {
        return DataReader.of(file, this, filter);
    }

    default DataWriter<T> newWriter(final File file) {
        return DataWriter.of(file, this);
    }

    default DataWriter<T> newWriter(final File file, final OperatorWithException<OutputStream> filter) {
        return DataWriter.of(file, this, filter);
    }

}

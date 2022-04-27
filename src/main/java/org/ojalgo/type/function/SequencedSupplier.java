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

import java.util.function.Supplier;

final class SequencedSupplier<T> implements AutoSupplier<T> {

    private Supplier<T> myCurrent;
    private int myIndex;
    private final Supplier<T>[] mySuppliers;

    SequencedSupplier(final Supplier<T>... suppliers) {

        super();

        mySuppliers = suppliers;
        if (suppliers.length > 0) {
            myIndex = 0;
            myCurrent = suppliers[myIndex];
        } else {
            myIndex = -1;
            myCurrent = null;
        }
    }

    @Override
    public void close() throws Exception {
        if ((myCurrent != null) && (myCurrent instanceof AutoCloseable)) {
            ((AutoCloseable) myCurrent).close();
        }
    }

    public T get() {

        if (myCurrent == null) {
            return null;
        }

        T retVal = myCurrent.get();

        if (retVal == null) {

            try {
                if (myCurrent instanceof AutoCloseable) {
                    ((AutoCloseable) myCurrent).close();
                }
                myIndex++;
            } catch (Exception cause) {
                throw new RuntimeException(cause);
            }

            if (myIndex >= mySuppliers.length) {
                myCurrent = null;
            } else {
                myCurrent = mySuppliers[myIndex];
                retVal = myCurrent.get();
            }
        }

        return retVal;
    }

}

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
package org.ojalgo.matrix.store;

import org.junit.jupiter.api.BeforeEach;
import org.ojalgo.random.Uniform;
import org.ojalgo.scalar.ComplexNumber;

public class SuperimposedRowCase extends NonPhysicalTest {

    @Override
    @BeforeEach
    public void setUp() {

        int tmpRowDim = Uniform.randomInteger(1, 9);
        int tmpColDim = Uniform.randomInteger(1, 9);

        MatrixStore<ComplexNumber> tmpBase = NonPhysicalTest.makeRandomMatrix(tmpRowDim, tmpColDim);
        MatrixStore<ComplexNumber> tmpRow = NonPhysicalTest.makeRandomMatrix(1, tmpColDim);
        int tmpIndex = Uniform.randomInteger(tmpRowDim);

        rationalStore = new SuperimposedStore<>(GenericStore.Q128.copy(tmpBase), tmpIndex, 0, GenericStore.Q128.copy(tmpRow));
        complexStore = new SuperimposedStore<>(GenericStore.C128.copy(tmpBase), tmpIndex, 0, GenericStore.C128.copy(tmpRow));
        primitiveStore = new SuperimposedStore<>(Primitive64Store.FACTORY.copy(tmpBase), tmpIndex, 0, Primitive64Store.FACTORY.copy(tmpRow));

        numberOfRows = tmpRowDim;
        numberOfColumns = tmpColDim;
    }

}

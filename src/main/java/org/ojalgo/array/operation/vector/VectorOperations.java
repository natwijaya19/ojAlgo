/*
 * Copyright 1997-2021 Optimatika
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
package org.ojalgo.array.operation.vector;

import jdk.incubator.vector.DoubleVector;
import jdk.incubator.vector.VectorOperators.Binary;
import jdk.incubator.vector.VectorOperators.Unary;
import jdk.incubator.vector.VectorSpecies;

/**
 * @author apete
 */
public final class VectorOperations {

    static final VectorSpecies<Double> SPECIES = DoubleVector.SPECIES_PREFERRED;

    public static void scalePlain(final double[] data, final double alpha) {
        VectorOperations.scalePlain(data, alpha, 0, data.length, 1);
    }

    public static void scaleVector(final double[] data, final double alpha) {

        int limit = data.length;
        int bound = SPECIES.loopBound(limit);
        int length = SPECIES.length();

        for (int i = 0; i < bound; i += length) {
            VectorOperations.scaleVector(data, alpha, i);
        }

        VectorOperations.scalePlain(data, alpha, bound, limit, 1);
    }

    public static void scaleVector(final double[] data, final double alpha, final int first, final int limit, final int step) {

        int bound = SPECIES.loopBound((limit - first) / step);
        int length = SPECIES.length();

        int[] indexMap = new int[bound];
        for (int i = 0; i < bound; i++) {
            indexMap[i] = first + i * step;
        }

        for (int i = 0; i < bound; i += length) {
            VectorOperations.scaleVector(data, alpha, 0, indexMap, i);
        }

        VectorOperations.scalePlain(data, alpha, first + bound * step, limit, step);
    }

    static void scalePlain(final double[] data, final double alpha, final int first, final int limit, final int step) {
        for (int i = first; i < limit; i += step) {
            data[i] *= alpha;
        }
    }

    static void scaleVector(final double[] data, final double alpha, final int offset) {
        DoubleVector.fromArray(SPECIES, data, offset).mul(alpha);
    }

    static void scaleVector(final double[] data, final double alpha, final int dataOffset, final int[] indexMap, final int mapOffset) {
        DoubleVector.fromArray(VectorOperations.SPECIES, data, dataOffset, indexMap, mapOffset).mul(alpha);
    }

    public static DoubleVector invoke(final DoubleVector target, final Unary operator) {
        return target.lanewise(operator);
    }

    public static DoubleVector invoke(final DoubleVector target, final Binary operator, final DoubleVector arg) {
        return target.lanewise(operator, arg);
    }

    public static DoubleVector invoke(final DoubleVector target, final Binary operator, final double arg) {
        return target.lanewise(operator, arg);
    }

}

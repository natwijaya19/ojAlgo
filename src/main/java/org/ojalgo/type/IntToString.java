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
package org.ojalgo.type;

public final class IntToString {

    static final String PADDING = "0000000000";

    static int numberOfDigits(int number) {
        if (number == 0) {
            return 1;
        }
        int count = 0;
        while (number != 0) {
            number /= 10;
            count++;
        }
        return count;
    }

    private final int myMaxNumberOfDigits;

    public IntToString(final int range) {
        super();
        myMaxNumberOfDigits = IntToString.numberOfDigits(range - 1);
    }

    /**
     * Will adjust the length of the input string to match what would have been created by
     * {@link #toString(int)} - assuming that if lengths don't match it's because of different number of
     * prefix zeros.
     */
    public String align(final String value) {
        if (value.length() == myMaxNumberOfDigits) {
            return value;
        }
        return this.toString(Integer.parseInt(value));
    }

    public String toString(final int value) {
        String retVal = Integer.toString(value);
        retVal = PADDING + retVal;
        return retVal.substring(retVal.length() - myMaxNumberOfDigits);
    }

}

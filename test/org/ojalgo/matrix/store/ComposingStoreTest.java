package org.ojalgo.matrix.store;

import org.junit.Before;
import org.junit.Test;
import org.ojalgo.function.NullaryFunction;

import static org.junit.Assert.*;
import static org.ojalgo.constant.PrimitiveMath.ONE;

public class ComposingStoreTest {
    private final static int BLOCK_COUNT = 128;
    private final static int SPARSE_SIZE = 128;
    private MatrixStore<Double> theMatrix;

    private MatrixStore<Double> sparseMatrix(int rowCount, int colCount){
        return sparseMatrix(rowCount, colCount, (int)Math.floor(Math.random()*rowCount*colCount*.075d));
    }

    private MatrixStore<Double> sparseMatrix(int rowCount, int colCount, int nonzeroCount){
        SparseStore<Double> matrix = SparseStore.makePrimitive(rowCount, colCount);
        for (int i = 0; i < nonzeroCount; i++) {
            int row = (int)Math.floor(Math.random()*rowCount);
            int col = (int)Math.floor(Math.random()*colCount);
            matrix.set(row, col, Math.random()*5+1);
        }
        return matrix;
    }

    private MatrixStore<Double> filledMatrix(int rowCount, int colCount){
        return filledMatrix(rowCount, colCount, 1d);
    }

    private MatrixStore<Double> filledMatrix(int rowCount, int colCount, double value){
        PhysicalStore.Factory<Double, PrimitiveDenseStore> storeFactory =
                PrimitiveDenseStore.FACTORY;
        return storeFactory.makeFilled(rowCount, colCount, new NullaryFunction<Double>() {

            public double doubleValue() {
                return ONE;
            }

            public Double invoke() {
                return ONE;
            }

        });
    }

    private MatrixStore<Double> blockMatrix(MatrixStore<Double> upperLeft, MatrixStore<Double> lowerRight){
        return  upperLeft.logical()
                .right((int)lowerRight.countColumns())
                .below(lowerRight.logical().left((int)upperLeft.countColumns()).get()).get();
    }

    @Before
    public void setUp(){
        theMatrix = sparseMatrix(SPARSE_SIZE,SPARSE_SIZE);
        for (int i = 0; i < BLOCK_COUNT; i++) {
            theMatrix = blockMatrix(theMatrix, filledMatrix(128, 128));
        }
    }

    @Test
    public void firstInColumn() {
        int block = (int)Math.floor(Math.random()*BLOCK_COUNT);
        assertEquals(block * 128 + SPARSE_SIZE, theMatrix.firstInColumn(block*128+SPARSE_SIZE+64));
    }

    @Test
    public void firstInRow() {
        int block = (int)Math.floor(Math.random()*BLOCK_COUNT);
        assertEquals(block * 128 + SPARSE_SIZE, theMatrix.firstInRow(block*128+SPARSE_SIZE+64));
    }

    @Test
    public void limitOfColumn() {
        int block = (int)Math.floor(Math.random()*BLOCK_COUNT);
        assertEquals((block +1)* 128 + 128, theMatrix.limitOfColumn(block*128+192));
    }

    @Test
    public void limitOfRow() {
        int block = (int)Math.floor(Math.random()*BLOCK_COUNT);
        assertEquals((block +1)* 128 + 128, theMatrix.limitOfRow(block*128+192));
    }
}
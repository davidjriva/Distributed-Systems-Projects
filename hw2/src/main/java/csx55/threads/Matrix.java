package csx55.threads;

import java.util.Random;
import java.util.Arrays;
import java.util.Collections;

public class Matrix {
    final int ROWS, COLS;
    final Random rand;
    int[][] values;
    
    public Matrix(int dimensionality){
        this.values = new int[dimensionality][dimensionality];
        this.ROWS = dimensionality;
        this.COLS = dimensionality;
        this.rand = new Random();
    }

    public Matrix(int dimensionality, Random rand) {
        this.values = new int[dimensionality][dimensionality];
        this.ROWS = dimensionality;
        this.COLS = dimensionality;
        this.rand = rand;
    }

    public void fillMatrix() {
        int upperBound = 1000;
        int lowerBound = -1000;

        for (int row = 0; row < ROWS; row++){
            for (int col = 0; col < COLS; col++) {
                values[row][col] = rand.nextInt(upperBound - lowerBound) + lowerBound;
            }
        }
    }

    public int[][] getValues() {
        return values;
    }

    public void setCell(int rowIndex, int colIndex, int value) {
        values[rowIndex][colIndex] = value;
    }

    @Override
    public String toString() {
        return Arrays.deepToString(values);
    }
}
package csx55.threads;

import java.util.Random;
import java.util.Arrays;
import java.util.Collections;

public class Matrix {
    private final int dimensionality;
    private final Random rand;
    private int[] values;
    private int[][] twoDValues;
    private String name;
    
    public Matrix(int dimensionality, String name){
        this.values = new int[dimensionality*dimensionality];
        this.dimensionality = dimensionality;
        this.rand = new Random();
        this.name = name;
    }

    public Matrix(int dimensionality, Random rand, String name) {
        this.values = new int[dimensionality*dimensionality];
        this.dimensionality = dimensionality;
        this.rand = rand;
        this.name = name;
    }

    public void fillMatrix() {
        int upperBound = 1000;
        int lowerBound = -1000;

        for (int row = 0; row < dimensionality; ++row){
            int offSet = row * dimensionality;
            for (int col = 0; col < dimensionality; ++col) {
                int location = offSet + col;
                values[location] = upperBound - rand.nextInt(upperBound - lowerBound);
            }
        }
    }

    public int[] getValues() {
        return values;
    }

    public int[][] getTwoDValues() {
        return twoDValues;
    }

    public String getName() {
        return name;
    }

    public void setCell(final int rowIndex, final int colIndex, final int value, final int offSet) {
        int location = offSet + colIndex;
        values[location] = value;
    }

    public void setRow(final int[] row, final int offSet) {
        System.arraycopy(row, 0, values, offSet, dimensionality);
    }

    public void toColumnWiseArray() {
        twoDValues = new int[dimensionality][dimensionality];

        for (int i = 0; i < dimensionality * dimensionality; ++i) {
            int row = i % dimensionality;
            int col = i / dimensionality;
            twoDValues[row][col] = values[i];
        }
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        result.append("[");

        for (int row = 0; row < dimensionality; row++) {
            int offset = row * dimensionality;
            result.append("[");
            for (int col = 0; col < dimensionality; col++) {
                int location = offset + col;
                result.append(values[location]);
                if (col < dimensionality - 1) {
                    result.append(", ");
                }
            }
            result.append("]");
            if (row < dimensionality - 1) {
                result.append(",\n");
            }
        }

        result.append("]");

        return result.toString();
    }
}
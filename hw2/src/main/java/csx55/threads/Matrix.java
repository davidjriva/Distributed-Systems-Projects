package csx55.threads;

import java.util.Random;
import java.util.Arrays;
import java.util.Collections;

public class Matrix {
    final int dimensionality;
    final Random rand;
    int[] values;
    
    public Matrix(int dimensionality){
        this.values = new int[dimensionality*dimensionality];
        this.dimensionality = dimensionality;
        this.rand = new Random();
    }

    public Matrix(int dimensionality, Random rand) {
        this.values = new int[dimensionality*dimensionality];
        this.dimensionality = dimensionality;
        this.rand = rand;
    }

    public void fillMatrix() {
        int upperBound = 1000;
        int lowerBound = -1000;

        for (int row = 0; row < dimensionality; row++){
            int offSet = row * dimensionality;
            for (int col = 0; col < dimensionality; col++) {
                int location = offSet + col;
                values[location] = upperBound - rand.nextInt(upperBound - lowerBound);
            }
        }
    }

    public int[] getValues() {
        return values;
    }

    public void setCell(int rowIndex, int colIndex, int value) {
        int offSet = rowIndex * dimensionality;
        int location = offSet + colIndex;
        values[location] = value;
    }

    /*  
    [1,2,3]     [1,4,7]
    [4,5,6] ->  [2,5,6]
    [7,8,9]     [3,6,9]
    
    [1,2,3,4,5,6,7,8,9]
    */
    public void toColumnWiseArray() {
        int[] tmp = Arrays.copyOf(values, values.length);

        for (int col = 0; col < dimensionality; col++) {
            for (int row = 0; row < dimensionality; row++) {
                values[row * dimensionality + col] = tmp[col * dimensionality + row];
            }
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
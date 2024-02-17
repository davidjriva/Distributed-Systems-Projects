package csx55.threads;

import java.util.Random;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.File;

public class MatrixThreads {
    private final int threadPoolSize, matrixDimension, seed;
    private final Random rand;
    private Matrix A, B, C, D, X, Y, Z;
    private ThreadPool threadPool;
    private AtomicInteger itemsProcessed;

    public MatrixThreads(int threadPoolSize, int matrixDimension, int seed) {
        this.threadPoolSize = threadPoolSize;
        this.matrixDimension = matrixDimension;
        this.seed = seed;
        this.rand = new Random(seed);
        this.itemsProcessed = new AtomicInteger(matrixDimension * matrixDimension);
    }

    private void initializeMatrices() {
        A = initializeMatrix();
        B = initializeMatrix();
        C = initializeMatrix();
        D = initializeMatrix();
        X = new Matrix(matrixDimension);
        Y = new Matrix(matrixDimension);
        Z = new Matrix(matrixDimension);
    }

    private Matrix initializeMatrix() {
        Matrix matrix = new Matrix(matrixDimension, rand);
        matrix.fillMatrix();
        return matrix;
    }

    /*
        A*B = C
        C(i,j) = Summation[A(i,k)*B(k,j)]

        Divides the matrix into [matrixDimension/(threadPoolSize/2)] sub-matrices that the threads then perform calculations on
    */
    private void multiplyMatrices(int[] m1, int[] m2, Matrix target) {
        // long startTime, endTime;
        for (int row = 0; row < matrixDimension; row++) {
            //startTime = System.nanoTime();
            int[] m1Row = getRow(m1, row);
            //endTime = System.nanoTime();
            //System.out.println("Row time= " + (endTime-startTime));
            for (int col = 0; col < matrixDimension; col++) {
                //startTime = System.nanoTime();
                int[] m2Col = getColumn(m2, col);
                //endTime = System.nanoTime();
                //System.out.println("Col time= " + (endTime - startTime));
                
                final int[] currRow = m1Row;
                final int[] currCol = m2Col;

                // Write to the matrix at this reference
                final int targetRow = row;
                final int targetCol = col;
                threadPool.addTask( () -> {
                    int res = 0;
                    for (int i = 0; i < currRow.length; i++) {
                        res += currRow[i] * currCol[i];
                    }
                    target.setCell(targetRow, targetCol, res);
                    itemsProcessed.getAndDecrement();
                });
            }  
        }
    }

    private int[] getRow(int[] values, int rowIndex) {
        int offSet = matrixDimension * rowIndex;
        return Arrays.copyOfRange(values, offSet, offSet + matrixDimension);
    }

    private int[] getColumn(int[] values, int colIndex) {
       int offSet = matrixDimension * colIndex;
       return Arrays.copyOfRange(values, offSet, offSet + matrixDimension);
    }

    private void initializeThreadPool() {
        int poolCapacity = 200 * threadPoolSize;
        threadPool = new ThreadPool(threadPoolSize, poolCapacity);
    }

    private void initializeItemsProcessed() {
        itemsProcessed = new AtomicInteger(matrixDimension * matrixDimension);
    }

    private void displayMatrixAfterCountDown(Matrix matrix, String matrixName) {
        // Busy wait for all items to be processed
        while (itemsProcessed.get() != 0) { }

        System.out.printf("Sum of the elements in input matrix %s = %d\n", matrixName, sumElementsInMatrix(matrix));
    }


    private long sumElementsInMatrix(Matrix m1) {
        int[] values = m1.getValues();
        // convert to stream, flatten stream, calculate sum
        return Arrays.stream(values).parallel().asLongStream().sum();
    }

    private double multiplyMatricesAndTime(Matrix m1, Matrix m2, Matrix target, String targetName) {
        //Reset item processed count
        initializeItemsProcessed();

        long startTime = System.currentTimeMillis();
        m2.toColumnWiseArray();
        multiplyMatrices(m1.getValues(), m2.getValues(), target);
        displayMatrixAfterCountDown(target, targetName);
        long endTime = System.currentTimeMillis();

        return ((endTime - startTime) / 1000.0);
    }

    private static void writeMatrixToFile(Matrix matrix, String fileName) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName))) {
            writer.write(matrix.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        int threadPoolSize = Integer.parseInt(args[0]);
        int matrixDimension = Integer.parseInt(args[1]);
        int seed = Integer.parseInt(args[2]);

        MatrixThreads matrixThreads = new MatrixThreads(threadPoolSize, matrixDimension, seed);

        System.out.printf("Dimensionality of the square matrices is: %d\n", matrixDimension);

        matrixThreads.initializeThreadPool();
        System.out.printf("The thread pool size has been initialized to: %d\n", threadPoolSize);

        matrixThreads.initializeMatrices();

        System.out.printf("Sum of the elements in input matrix A = %d\n", matrixThreads.sumElementsInMatrix(matrixThreads.A));
        System.out.printf("Sum of the elements in input matrix B = %d\n", matrixThreads.sumElementsInMatrix(matrixThreads.B));
        System.out.printf("Sum of the elements in input matrix C = %d\n", matrixThreads.sumElementsInMatrix(matrixThreads.C));
        System.out.printf("Sum of the elements in input matrix D = %d\n\n", matrixThreads.sumElementsInMatrix(matrixThreads.D));

        double XCalculationTimer = matrixThreads.multiplyMatricesAndTime(matrixThreads.A, matrixThreads.B, matrixThreads.X, "X");
        System.out.printf("Time to compute matrix X is: %.3fs\n\n", XCalculationTimer);

        double YCalculationTimer = matrixThreads.multiplyMatricesAndTime(matrixThreads.C, matrixThreads.D, matrixThreads.Y, "Y");
        System.out.printf("Time to compute matrix Y is: %.3fs\n\n", YCalculationTimer);

        double ZCalculationTimer = matrixThreads.multiplyMatricesAndTime(matrixThreads.X, matrixThreads.Y, matrixThreads.Z, "Z");
        System.out.printf("Time to compute matrix Z is: %.3fs\n\n", ZCalculationTimer);

        System.out.printf("Time to compute matrices X, Y, and Z using a thread pool of size = <%d> is : %.3fs\n", threadPoolSize, XCalculationTimer + YCalculationTimer + ZCalculationTimer);

        matrixThreads.threadPool.stop();
    }
}
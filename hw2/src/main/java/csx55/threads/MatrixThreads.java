package csx55.threads;

import java.util.Random;
import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.File;

public class MatrixThreads {
    private final int threadPoolSize, matrixDimension, seed;
    private final Random rand;
    private Matrix A, B, C, D, X, Y, Z;
    private ThreadPool threadPool;
    private AtomicInteger operationsLeft;

    public MatrixThreads(int threadPoolSize, int matrixDimension, int seed) {
        this.threadPoolSize = threadPoolSize;
        this.matrixDimension = matrixDimension;
        this.seed = seed;
        this.rand = new Random(seed);
        this.operationsLeft = new AtomicInteger(matrixDimension * matrixDimension);
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
        
        model name      : 12th Gen Intel(R) Core(TM) i7-12700K
        cache size      : 25600 KB
    */
    private void multiplyMatrices(final int[] m1, final int[][] m2, final Matrix target) {
        final int operationDelta = -1 * matrixDimension;
        for (int row = 0; row < matrixDimension; ++row) {
            final int targetRow = row;
            final int[] m1Row = getRowOrCol(m1, targetRow);
            final int offSet = row * matrixDimension;

            threadPool.addTask( () -> {
                int[] res = new int[matrixDimension];
                for (int col = 0; col < matrixDimension; ++col) {
                    res[col] = calculateDotProduct(m1Row, m2[col]);
                }
                target.setRow(res, offSet);
                operationsLeft.addAndGet(operationDelta);
            });
        }
    }

    private int calculateDotProduct(final int[] v1, final int[] v2) {
        int res = 0;
        for (int i = 0; i < v1.length; ++i) {
            res += v1[i] * v2[i];
        }
        return res;
    }

    // Matrix is stored col-major format when this is called so the rows represent the columns of the original matrix.
    private int[] getRowOrCol(final int[] values, final int rowOrColIndex) {
        int[] row = new int[matrixDimension];
        int offSet = matrixDimension * rowOrColIndex;

        System.arraycopy(values, offSet, row, 0, matrixDimension);

        return row;
    }

    private void initializeThreadPool() {
        int poolCapacity = 200 * threadPoolSize;
        threadPool = new ThreadPool(threadPoolSize, poolCapacity);
    }

    private void initializeItemsProcessed() {
        operationsLeft = new AtomicInteger(matrixDimension * matrixDimension);
    }

    private long sumElementsInMatrix(final Matrix m1) {
        int[] values = m1.getValues();
        // convert to stream, flatten stream, calculate sum
        return Arrays.stream(values).parallel().asLongStream().sum();
    }

    private double multiplyMatricesAndTime(final Matrix m1, final Matrix m2, final Matrix target, final String targetName) {
        //Reset item processed count
        initializeItemsProcessed();

        // Perform the multiplication calculation
        long startTime = System.currentTimeMillis();
        m2.toColumnWiseArray();
        multiplyMatrices(m1.getValues(), m2.getTwoDValues(), target);
        displayMatrixAfterCountDown(target, targetName);
        long endTime = System.currentTimeMillis();

        return ((endTime - startTime) / 1000.0);
    }

    private void displayMatrixAfterCountDown(final Matrix matrix, final String matrixName) {
        // Busy wait for all items to be processed
        while (operationsLeft.get() != 0) { }

        System.out.printf("Sum of the elements in input matrix %s = %d\n", matrixName, sumElementsInMatrix(matrix));
    }

    private static void writeMatrixToFile(final Matrix matrix, final String fileName) {
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
        System.out.printf("The thread pool size has been initialized to: %d\n\n", threadPoolSize);

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
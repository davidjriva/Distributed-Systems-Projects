package csx55.threads;

import java.util.Random;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;

public class MatrixThreads {
    private final int ROWS, COLS, threadPoolSize, matrixDimension, seed;
    private final Random rand;
    private Matrix A, B, C, D, X, Y;
    private ThreadPool threadPool;
    private CountDownLatch latch;

    public MatrixThreads(int threadPoolSize, int matrixDimension, int seed) {
        this.threadPoolSize = threadPoolSize;
        this.matrixDimension = matrixDimension;
        this.seed = seed;
        this.rand = new Random(seed);
        this.ROWS = matrixDimension;
        this.COLS = matrixDimension;
        this.latch = new CountDownLatch(matrixDimension * matrixDimension);
    }

    private void initializeMatrices() {
        A = initializeMatrix();
        B = initializeMatrix();
        C = initializeMatrix();
        D = initializeMatrix();
        X = new Matrix(matrixDimension);
        Y = new Matrix(matrixDimension);
    }

    private Matrix initializeMatrix() {
        Matrix matrix = new Matrix(matrixDimension, rand);
        matrix.fillMatrix();
        return matrix;
    }

    /*
        A*B = C
        C(i,j) = Summation[A(i,k)*B(k,j)]
    */
    private void multiplyMatrices(int[][] m1, int[][] m2, Matrix target) {
        for (int row = 0; row < ROWS; row++) {
            int[] m1Row = getRow(m1, row);
            
            for (int col = 0; col < COLS; col++) {
                int[] m2Col = getColumn(m2, col);
                
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
                    latch.countDown();
                });
            }  
            // Get all columns then at the end call addTask():
            
        }
    }

    private int[] getRow(int[][] matrix, int rowIndex) {
        return matrix[rowIndex];
    }

    private int[] getColumn(int[][] matrix, int colIndex) {
        int[] column = new int[ROWS];

        for (int innerRow = 0; innerRow < ROWS; innerRow++) {
            column[innerRow] = matrix[innerRow][colIndex];
        }
        return column;
    }

    private void initializeThreadPool() {
        int poolCapacity = 50 * threadPoolSize;
        threadPool = new ThreadPool(threadPoolSize, poolCapacity);
    }

    private void displayX() {
        try {
            latch.await();
        } catch(InterruptedException ie) {
            System.err.println("MatrixThreads.java " + ie.getMessage());
        }

        System.out.println("Sum of the elements in input matrix X = " + sumElementsInMatrix(X));
    }

    private int sumElementsInMatrix(Matrix m1) {
        int[][] values = m1.getValues();
        // convert to stream, flatten stream, calculate sum in parallel
        return Arrays.stream(values).parallel().flatMapToInt(Arrays::stream).sum();
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
        System.out.printf("Sum of the elements in input matrix D = %d\n", matrixThreads.sumElementsInMatrix(matrixThreads.D));

        long startTime = System.currentTimeMillis();
        matrixThreads.multiplyMatrices(matrixThreads.A.getValues(), matrixThreads.B.getValues(), matrixThreads.X);
        matrixThreads.displayX();
        long endTime = System.currentTimeMillis();

        System.out.printf("Time to compute matrix X: %.3fs\n", ((endTime - startTime)/1000.0));
    }
}
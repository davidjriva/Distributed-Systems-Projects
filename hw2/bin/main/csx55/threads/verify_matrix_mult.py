'''
    Script to verify if matrix math is correct.
    arg1 - matrix 1 file (formatted as python 2d list)
    arg2 - matrix 2 file (formatted as python 2d list)
    arg3 - output file from program (formatted as python 2d list)
'''

import sys
import dask.array as da
import numpy as np
import re

def matrix_multiply(mat1, mat2):
    return da.dot(mat1, mat2).compute()
    
def are_matrices_equal(mat1, mat2):
    return da.equal(mat1, mat2).all().compute()

def read_matrix_from_file(filename):
    with open(filename, 'r') as file:
        content = file.read()
        # Extracting numeric values using regular expression
        numeric_values = re.findall(r'-?\d+', content)
        # Converting numeric values to integers
        numeric_values = list(map(int, numeric_values))
        # Assuming a square matrix, calculating the side length
        side_length = int(np.sqrt(len(numeric_values)))
        # Creating the matrix
        matrix = np.array(numeric_values).reshape((side_length, side_length))
        dask_array = da.from_array(matrix, chunks=(500, 500))
    return dask_array


if __name__ == "__main__":
    matrix_1_filename = sys.argv[1]
    matrix_2_filename = sys.argv[2]
    actual_filename = sys.argv[3]

    print('reading matrices from file...')
    matrix1 = read_matrix_from_file(matrix_1_filename)
    print('Matrix 1 shape: ', matrix1.shape, ', Sum: ', matrix1.sum().compute())

    matrix2 = read_matrix_from_file(matrix_2_filename)
    print('Matrix 2 shape: ', matrix2.shape, ', Sum: ', matrix2.sum().compute())

    expected_matrix = matrix_multiply(matrix1, matrix2)
    print('Expected matrix shape: ', expected_matrix.shape, ', Sum: ', expected_matrix.sum())

    actual_matrix = read_matrix_from_file(actual_filename)
    print('Actual matrix shape: ', actual_matrix.shape, ', Sum: ', actual_matrix.sum().compute())

    print('finished reading matrices from file!')

    print('determining if matrices are equal...')
    print('equal= ', are_matrices_equal(expected_matrix, actual_matrix))

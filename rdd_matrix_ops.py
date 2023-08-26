from pyspark import RDD


def vector_distance(vector1: RDD, vector2: RDD):
    """
    Parameters
    ----------
    vector1: pyspark.RDD
        v1_j := (j, v1_j) where v1 is vector1 and j is row idx
    vector2: pyspark.RDD
        v2_j := (j, v2_j) where v2 is vector2 and j is row idx

    Returns
    -------
    distance: float
        distance := ||v1-v2||
    """
    distance = vector1. \
               join(vector2). \
               map(lambda x: (x[1][0] - x[1][1])**2). \
               sum()**0.5
    return distance

def matrix_vector_multiplication(matrix: RDD, vector: RDD):
    """
    Parameters
    ----------
    matrix: pyspark.RDD
        M_ij := (j, (i, M_ij)) where M is matrix and i, j is row, column idx
    vector: pyspark.RDD
        v_j := (j, v_j) where v is vector and j is row idx

    Returns
    -------
    result_vector: pyspark.RDD
        v_i := (i, (M_ij * v_j))
    """
    # matrix.join(vector) -> (j, ((i, M_ij), v_j))
    result_vector = matrix. \
                    join(vector). \
                    map(lambda x: (x[1][0][0], x[1][0][1] * x[1][1])). \
                    reduceByKey(lambda x, y: x + y)
    return result_vector
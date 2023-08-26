from pyspark import SparkContext

from rdd_matrix_ops import matrix_vector_multiplication, vector_distance


def calculate_page_ranks(sc: SparkContext,
                         text_file: str,
                         option: int = 1,
                         beta: float = 0.85,
                         tol: float = 1e-4,
                         iterations: int = 100,
                         verbose=False):
    """
    Parameters
    ----------
    text_file: str
        file containing lines with space separated ids (representing pages and
        link from first id to second)
    option: int
        -> 1 => terminate if change in page ranks below tolerance 'tol' passed
        -> 2 => run for 'iterations' iterations
        -> Any other value => ignore algorithm
        default = 1
    beta: float
        1 - beta represents teleportation factor
        default = 0.85
    tol: float
        tolerance to be checked to terminate iterations for option = 1
        default = 1e-4
    iterations: int
        no. of iterations to run the algorithm for
        default = 15

    Returns
    -------
    distribution_vector: pyspark.RDD
        page rank vector v_i := (i, (M_ij * v_j))
    """
    lines = sc.textFile(text_file)
    link_list = lines. \
                map(lambda line: tuple(line.split()))
    num_partitions = link_list.getNumPartitions()
    pages = link_list.keys(). \
            union(link_list.values()). \
            distinct(). \
            coalesce(num_partitions)

    num_pages = pages.count()
    distribution_vector = pages. \
                          map(lambda page: (page, 1/num_pages))

    out_degrees = link_list. \
                  map(lambda page: (page[0], 1)). \
                  reduceByKey(lambda x, y: x+y)
    transition_matrix = link_list.join(out_degrees). \
                        map(lambda x: (x[0], (x[1][0], 1/x[1][1]))). \
                        coalesce(num_partitions). \
                        cache()
    iteration = 0
    if option == 2:
        for _ in range(iterations):
            new_distribution_vector = \
            matrix_vector_multiplication(transition_matrix, distribution_vector)
            new_distribution_vector = new_distribution_vector. \
                                      coalesce(num_partitions). \
                                      mapValues(lambda prob:
                                                beta * prob +
                                                (1 - beta) / num_pages)
            distribution_vector = new_distribution_vector
            if verbose:
                print(f'Completed iteration: {iteration}')
            iteration += 1
    elif option == 1:
        while True:
            new_distribution_vector = \
            matrix_vector_multiplication(transition_matrix, distribution_vector)
            new_distribution_vector = new_distribution_vector. \
                                      mapValues(lambda prob:
                                                beta * prob +
                                                (1 - beta) / num_pages)
            dist = vector_distance(new_distribution_vector, distribution_vector)
            distribution_vector = new_distribution_vector. \
                                  coalesce(num_partitions)
            if verbose:
                print(f'Completed iteration: {iteration}',
                      f'Norm of change: {dist}')
            if dist < tol:
                break
            iteration += 1
    return distribution_vector
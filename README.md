# rdd-pyspark-pagerank
PageRank algorithm implementation with Pyspark

Motivated by the unavailability of graphx - pagerank in Pyspark - [https://issues.apache.org/jira/browse/SPARK-3789]
<br>
Note: Graphframes has an implementation - "https://graphframes.github.io/graphframes/docs/_site/api/python/graphframes.html#graphframes.GraphFrame.pageRank"

This project implements using Python(+Pyspark), the common iterative versions of PageRank algorithm (1. termination based on change in page ranks vector, 2. termination based on number of iterations)
<br>
Ref: [https://en.wikipedia.org/wiki/PageRank#Iterative]

Performance: 
-
- Comparison with Scala
- Test case attempted for dataset: https://snap.stanford.edu/data/ego-Twitter.html 
- It took approximately 8-9 times slower than Scala's graphx library implementation - which is still better than the colloquial metric that Python performance is 10x slower than Python.
- The scores are different for Scala, because the implementation is different. But the relative order matched exactly.

Pending issues and future updates:
-
- When the number of iterations for PageRank computation is too high (option 2), it results in stack overflow error.
  
  Planned solution: Introduce RDD checkpointing

# rdd-pyspark-pagerank
PageRank algorithm implementation with Pyspark

Motivated by the unavailability of graphx - pagerank in Pyspark - [https://issues.apache.org/jira/browse/SPARK-3789]
<br>
Note: Graphframes has an implementation - "https://graphframes.github.io/graphframes/docs/_site/api/python/graphframes.html#graphframes.GraphFrame.pageRank"

This project implements using Python(+Pyspark), the common iterative versions of PageRank algorithm (1. termination based on number of iterations, 2. termination based on change in page ranks)
<br>
Ref: [https://en.wikipedia.org/wiki/PageRank#Iterative]

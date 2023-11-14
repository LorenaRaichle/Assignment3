Assignment 3
------------

# Team Members : Joey & Lorena

# GitHub link to your repository (if submitting through GitHub)

https://github.com/LorenaRaichle/Assignment3

Please note: for the code update of task 3 ChatGPT has been used. 
...

# Task 2

Performance difference is ...
A: When comparing the performance of the execution in cluster mode (task2) versus local mode (task 1) the improvement of performance for the cluster mode set up is achieved because of the following arguments:
Since in the cluster mode, the workload is spread across multiple nodes / workers, there is an improvement in performance in comparison to the local mode setup.
In addition to that, the cluster mode is able to deal with node failures and redistribute the task to other nodes (fault tolerance).
Also, the cluster mode can execute tasks in parallel across different nodes, which leads to a faster execution. 
These advantages are especially important to consider for large datasets. Since the given input file was rather small, the performance improvement in the implementation from task1 in local mode and task 2 in cluster mode was not significant. 
# Task 3

1. How does Spark optimize its file access compared to the file access in MapReduce?
> Ans: 
- Spark performs in-memory processing and stores the intermediate data in random access memory (RAM), whereas MapReduce persists data back to the disk after a map or reduce action. This reduces the amount of read/write actions for Spark, resulting in a faster processing speed especially for iterative algorithms. 
- Spark can persist (cache) intermediate data in memory or on disk. This enables the reuse of data across multiple stages of computation, reducing the need to recompute the same data. In MapReduce, intermediate data is always read from and written directly to the disk, and there is no built-in mechanism for caching data.
- Spark features an advanced directed acyclic graph (DAG) engine supporting cyclic data flow, allowing it to optimize and schedule the execution of operations effectively. Each Spark job creates a DAG of task stages to be performed on the cluster. Compared to MapReduce, which creates a DAG with two predefined stages - Map and Reduce, DAGs created by Spark can contain any number of stages. This allows some jobs to complete faster than they would in MapReduce, with simple jobs completing after just one stage, and more complex tasks completing in a single run of many stages, rather than having to be split into multiple jobs.

2. In your implementation of WordCount (task1), did you use ReduceByKey or groupByKey method? 
   > Ans:
- In our implementation we used ReduceByKey for efficiency advantages. The reduction / summing of word counts will be performed on each group before shuffling the data. Therefore, less data has to be send in the network. 
What does your preferred method do in your implementation? 
> Ans:
- In the given code, first the flatmap transformation is applied to the input data to generate the dataset words (Java RDD).
- Then the mapToPair method is used to map each word to a key value pair with the key = word and value = 1 for every pair.
- The ReduceByKey method is used to merge the values for each key (adding up counts for each word) on each partition. This minimizes the amount of data transfer that is necessary.

What are the differences between the two methods in Spark?
> Ans: 
- groupByKey groups the values corresponding to each key and returns an RDD of key-value pairs, where each value is an iterable collection of the values corresponding to that key. On the other hand, reduceByKey groups the values corresponding to each key and then applies a reduction function to the values of each group, returning an RDD of key-value pairs, where each value is the result of the reduction operation for the values corresponding to that key.
- Since ReduceByKey performs the merging operation / adding up the word counts for each word locally on each partition first before sending it, fewer data has to be transferred which is more efficient. This is especially relevant, when the amount of processed data increases.

3. Explain what Resilient Distributed Dataset (RDD) is and the benefits it brings to the classic MapReduce model.
> Ans:
- A Resilient Distributed Dataset (RDD) is a data structure of Apache Spark, an immutable distributed collection of datasets partitioned across a set of nodes of the cluster that can be recovered if a partition is lost, thus providing fault tolerance. It also provides in-built memory computing and referencing datasets stored in external storage systems, reducing the reliance on disk for intermediate data storage. This can lead to a significant performance improvement as compared to the storage and processing model of the traditional MapReduce.

4. Imagine that you have a large dataset that needs to be processed in parallel. 
   How would you partition the dataset efficiently and keep control over the number of outputs created at the end of the execution

  If a task is stuck on the Spark cluster due to a network issue that the cluster had during execution, 
  which methods can be used to retry or restart the task execution on a node?
> Ans:
- We can utilise hash partitioning in Spark which spreads the data based on a hash function. Data is spread evenly across various partitions, on the basis of a key. To determine the partition in Spark we use Object.hashCode method: partition = key.hashCode () % numPartitions. - Both ‘coalesce’ or ‘repartition’ can be used with hash partitioning to control the number of outputs. ‘coalesce’ can reduce the number of partitions without a full shuffle but there could be uneven data distribution across partitions. ‘repartition’ can ensure a more uniform distribution of data across partitions by controlling the number of partitions and performing a full shuffle.
- If a task is stuck on the Spark cluster due to a network issue, we can use the built-in mechanism in Spark for retrying failed tasks. This can be achieved by setting the spark.task.maxFailures configuration property to retry failed tasks on different nodes. We could also use the Spark driver logs to monitor the occurrence of a stage failure and manually resubmit the failed stage or job.



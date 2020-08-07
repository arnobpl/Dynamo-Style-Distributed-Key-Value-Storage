# Dynamo-Style Distributed Key-Value Storage
This is a distributed key-value storage with partitioning, replication, and failure handling. It is a simple version of [Amazon Dynamo](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf "Paper link"). The main goal of the distributed key-value storage system is to provide both availability and linearizability at the same time. In other words, the implementation should always perform read and write operations successfully even under failures (i.e., availability). At the same time, a read operation should always return the most recent value (i.e., linearizability).

## Features

1. **Operations:** It supports insert, delete, and query operations with any key. For the query operation, it also supports to return all the key-value pairs from the whole system (i.e., all the nodes). Moreover, it supports to return all the key-value pairs (including the replicated pairs) from the handling node.

2. **Partitioning:** There are always five nodes in the system. Currently, it does not support adding/removing nodes to/from the system.

3. **Routing:** Unlike the [Chord distributed hash table](https://pdos.csail.mit.edu/papers/ton:chord/paper-ton.pdf "Paper link"), each node in the system knows every other node. Each node also knows exactly which partition belongs to which node.

4. **Consistency:** It guarantees the consistency of per-key linearizability. In other words, a read operation on a specific key should always return the most recent value of that key regardless of the requested node that handles the read operation.

5. **Concurrency:** It supports concurrent read/write operations.

6. **Availability:** It can handle a failure happening at the same time with read/write operations. That means, if one node fails, other nodes can still serve read/write requests correctly (i.e., per-key linearizability).

7. **Failure handling:** There can be at most one node failure at any given time. All failures are temporary. That means the failed node will not be permanently unavailable during a run.

8. **Failure recovery:** When a node recovers after failure, it can copy all the object writes it missed during the failure.

9. **Replication:** Replication is done exactly the same way as Dynamo does. In other words, a key-value pair is replicated over three consecutive partitions, starting from the partition that the key belongs to. Replication is implemented using the [Chain replication](https://www.cs.cornell.edu/home/rvr/papers/OSDI04.pdf "Paper link") strategy.

## Acknowledgement

This project was done as a part of [CSE 586 Distributed Systems](https://cse.buffalo.edu/~stevko/courses/cse486/spring20 "Course page") course instructed by Professor [Dr. Steve Ko](https://cse.buffalo.edu/~stevko "Faculty page"), Department of [Computer Science and Engineering](https://cse.buffalo.edu "Department page"), University at Buffalo.

# 关系型数据库管理系统
在关系型数据库管理系统BusTub(https://github.com/cmu-db/bustub)的基础上进行部分模块实现。
主要工作：
•	实现缓冲池管理器：实现LRU-K作为缓冲页替换策略; 实现缓冲页的创建、加载、删除、pin/unpin以及刷新
•	使用Extendible Hashing实现哈希索引功能
•	使用迭代器模型实现一部分查询执行器，包括SeqScan, Insert, Update, Delete, IndexScan, Aggregation, NestedLoopJoin, HashJoin, Sort, Limit, Top-N, Window Functions
•	实现部分查询计划优化功能，包括优化SeqScan为IndexScan, 优化NestedLoopJoin为HashJoin, 优化Sort + Limit为Top-N
•	实现MVOCC (Optimistic Multi-version Concurrency Control)：在已有的类似Delta Storage的版本存储模型基础上:  将MVCC引入之前实现的查询执行器，包括SeqScan, Insert, Update, Delete, Index Scan; 实现事务的commit/abort; 实现版本垃圾回收; 实现serializable isolation level下的序列化验证

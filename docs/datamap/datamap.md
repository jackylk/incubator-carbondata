# CarbonData DataMap

## DataMap Implementation

DataMap can be created using following DDL

```
  CREATE DATAMAP [IF NOT EXISTS] datamap_name
  [ON TABLE main_table]
  USING "datamap_provider"
  DMPROPERTIES ('key'='value', ...)
  AS
    SELECT statement
```

Currently, there are 4 kinds of datamap. 

| DataMap provider | Description                      | DMPROPERTIES                                                 |Management |
| ---------------- | -------------------------------- | ------------------------------------------------------------ |-----------|
| preaggregate     | single table pre-aggregate table | No DMPROPERTY is required                                    |Automatic  |
| timeseries       | time dimension rollup table.     | event_time, xx_granularity, please refer to [Timeseries DataMap](https://github.com/apache/carbondata/blob/master/docs/datamap/timeseries-datamap-guide.md) |Automatic |
| mv               | multi-table pre-aggregate table, | No DMPROPERTY is required                                    |Manual |
| lucene           | lucene indexing for text column                  | text_column                                |Manual |
| bloom           | bloom filter for high cardinality column, geospatial column                   | bloom_column   |Manual |

## DataMap Management

There are three kinds of semantic for DataMap management:

### Automatic Refresh

All datamap except MV datamap (preaggregate/timeseries/lucene) is in this category.

When a user creates a datamap on the main table, system will immediately triger a datamap refresh automatically. It is triggered internally by the system, and the user does not need to issue REFRESH command.

Afterwards, for every new data loading, system will immediatly trigger an incremental refresh on all related datamap created on the main table. The refresh is **incremental** based on Segment concept of Carbon.

If user perform data update, delete operation, system will return failure. (reject the operation)

If user drop the main table, the datamap will be dropped immediately too.

### Manual Refresh

MV datamap is in this category. 

When user creates a mv datamap on multiple table, the datamap is created with status *disabled*, then user can issue REFRESH command to build the datamap. Every REFRESH command that user issued, system will trigger a full rebuild of the datamap. After rebuild is done, system will change datamap status to *enabled*, so that it can be used in query rewrite.

For every new data loading, data update, delete, the related datamap will be *disabled*.

If the main table is dropped by user, the related datamap will be dropped immediately.

In future, following feature should be supported in MV datamap (these currently is support in preaggregate datamap):

1. Incremental refresh, for single table case
2. Partitioning
3. Query rewrite on streaming table (union of groupby query on streaming segment and scan query on datamap table)

### Manual Management

If you are creating a datamap on external table, you need to do all manual managment of the datamap. It means you should do:

- After you create datamap on the external table, if you again load new data, update data, delete data, drop table, you need to drop the datamap immediately, so that query is consistent with the latest main table data.
- If you want to use datamap after above mentioned operation, you should create the datamap again. (For MV datamap, create datamap and issue REFRESH explicitly)

## DataMap Definition

There is no restriction on the datamap definition, the only restriction is that the main table need to be created before hand.

User can create datamap on managed table or unmanaged table (external table)

## Query rewrite using DataMap

How can user know whether datamap is used in certain query?

User can use EXPLAIN command to know, it will print out something like

```text
== CarbonData Profiler == 
Hit mv DataMap: datamap1
Scan Table: default.datamap1_table
+- filter: 
+- pruning by CG DataMap
+- all blocklets: 1
   skipped blocklets: 0
```

## DataMap Catalog

Currently, when user creates a datamap, system will store the datamap metadata in a configurable *system* folder in HDFS or S3.  

In this *system* folder, it contains:

- DataMapSchema file. It is a json file containing schema for one datamap. Ses DataMapSchema class. If user creates 100 datamaps (on different tables), there will be 100 files in *system* folder. 
- DataMapStatus file. Only one file, it is in json format, and each entry in the file represents for one datamap. Ses DataMapStatusDetail class

There is a DataMapCatalog interface to retrieve schema of all datamap, it can be used in optimizer to get the metadata of datamap. 

### Show DataMap

There is a SHOW DATAMAPS command, when this is issued, system will read all datamap from *system* folder and print all information on screen. The current information includes:

- DataMapName
- DataMapProviderName like mv, preaggreagte, timeseries, etc
- Associated Table

##Compaction on DataMap

This feature applies for preaggregate datamap only

Running Compaction command (`ALTER TABLE COMPACT`) on main table will **not automatically** compact the pre-aggregate tables created on the main table. User need to run Compaction command separately on each pre-aggregate table to compact them.

Compaction is an optional operation for pre-aggregate table. If compaction is performed on main table but not performed on pre-aggregate table, all queries still can benefit from pre-aggregate tables. To further improve the query performance, compaction on pre-aggregate tables can be triggered to merge the segments and files in the pre-aggregate tables. 

## Data Management on Main Table
In current implementation, data consistence need to be maintained for both main table and pre-aggregate tables. Once there is pre-aggregate table created on the main table, following command on the main table is not supported:
1. Data management command: `UPDATE/DELETE/DELETE SEGMENT`. 
2. Schema management command: `ALTER TABLE DROP COLUMN`, `ALTER TABLE CHANGE DATATYPE`, 
  `ALTER TABLE RENAME`. Note that adding a new column is supported, and for dropping columns and 
  change datatype command, CarbonData will check whether it will impact the pre-aggregate table, if 
   not, the operation is allowed, otherwise operation will be rejected by throwing exception.   
3. Partition management command: `ALTER TABLE ADD/DROP PARTITION`

However, there is still way to support these operations on main table, user can do as following:
1. Remove the pre-aggregate table by `DROP DATAMAP` command
2. Carry out the data management operation on main table
3. Create the pre-aggregate table again by `CREATE DATAMAP` command
  Basically, user can manually trigger the operation by re-building the datamap.




## Immediate MV requirements

0. User scenario: how would a user interact with the system and use MVs?
1. DataMapCatalog should be place in DB instead of using HDFS files. This includes the MV definition, the owner/creator, the status, and perhaps some stats.  We also need to expose MVs to users so that one can query them as if they are based tables, and yet one cannot update them.
2. Provide an interface for DataMapCatalog to manage the DataMap metadata, including listing all MVs and updating the status/stats.
3. We need to add tenant information or different place for different tenant in the DataMap Catalog, so that we can retrieve datamap for given tenant. 
4. Focus on single table preaggregate advisor. Improve advisor capability like adding table size reduction estimation, query latency reduction estimation, by leveraging statistics in CBO
5. Migrate preaggregate datamap to MV datamap framework (ModularPlan framework), and move partition/streaming feature to MV datamap.
6. Testing, including concurrent queries/matching, concurrent create/drop
7. Left join in MV and extend the matching algorithm.
8. Provide a fast way to match the MV, considering 1000s of MV (resuing query result). Idea: 1)signature (like hashcode) of the given table. 2) some ML algorithm like decision tree, etc.  We may need a cache of MVs that have been converted into modular plans in order to avoid repeated query compilatin/optimization.
9. query result MV management: need to know if the MV is becoming stale. Need to consider it in DataMapCatalog also.

Additional future requirements:
1. Extend the matching capability (depending on customer scenarios), like count distinct, more aggregate functions, more join support, more complex subquery or groupby expressions
2. Extend the matching for join (wide table, can benefit BI): should support join rewrite using mv with lossless joins (mv: F Join D1 and D2; query: F Join D1; can rewrite the query to using mv if mv joins are lossless)


/*
 * Copyright 2010 Michael Fortin <mike@brzy.org>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");  you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed 
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR 
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */
package org.brzy.calista

/**
 * A description of the Family, including all it's attributes.
 *
 * This is just a proxy to the underlying casandra thrift object which looks like this:
 * {{{
 * /* describes a column family. */
 * struct CfDef {
 *     1: required string keyspace,
 *     2: required string name,
 *     3: optional string column_type="Standard",
 *     5: optional string comparator_type="BytesType",
 *     6: optional string subcomparator_type,
 *     8: optional string comment,
 *     9: optional double row_cache_size=0,
 *     11: optional double key_cache_size=200000,
 *     12: optional double read_repair_chance=1.0,
 *     13: optional list<ColumnDef> column_metadata,
 *     14: optional i32 gc_grace_seconds,
 *     15: optional string default_validation_class,
 *     16: optional i32 id,
 *     17: optional i32 min_compaction_threshold,
 *     18: optional i32 max_compaction_threshold,
 *     19: optional i32 row_cache_save_period_in_seconds,
 *     20: optional i32 key_cache_save_period_in_seconds,
 *     21: optional i32 memtable_flush_after_mins,
 *     22: optional i32 memtable_throughput_in_mb,
 *     23: optional double memtable_operations_in_millions,
 *     24: optional bool replicate_on_write,
 *     25: optional double merge_shards_chance,
 *     26: optional string key_validation_class,
 *     27: optional string row_cache_provider="org.apache.cassandra.cache.ConcurrentLinkedHashCacheProvider",
 *     28: optional binary key_alias,
 * }
 * }}}
 *
 * @see SessionManager.keyspaceDefinition
 * @author Michael Fortin
 */
case class FamilyDefinition(
    name:String,
    columnType:String,
    comparatorType:String,
    subcomparatorType:String,
    comment:String,
    rowCacheSize:Double,
    keyCacheSize:Double)
package org.brzy.calista.system

import org.apache.cassandra.thrift.CfDef
import collection.JavaConversions._

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
/**
 * A description of the Family, including all it's attributes.
 *
 * This is just a proxy to the underlying casandra thrift object which looks like this:
 * {{{
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
class FamilyDefinition(
    val name:String,
    val columnType:String = "Standard",
    val comparatorType: Option[String ] = Option("BytesType"),
    val subcomparatorType: Option[String] = None,
    val comment: Option[String] = None,
    val rowCacheSize: Option[Double ] = Option(0.0),
    val keyCacheSize: Option[Double ] = Option(200000),
    val readRepairChance: Option[Double ] = Option(1.0),
    val columnMetadata: Option[List[ColumnDefinition] ] = None,
    val gcGraceSeconds: Option[Int] = None,
    val defaultValidationClass: Option[String] = None,
    val id: Option[Int] = None,
    val minCompactionThreshold: Option[Int] = None,
    val maxCompactionThreshold: Option[Int] = None,
    val rowCacheSavePeriodInSeconds: Option[Int] = None,
    val keyCacheSavePeriodInSeconds: Option[Int] = None,
    val memtableFlushAfterMins: Option[Int] = None,
    val memtableThroughputInMb: Option[Int] = None,
    val memtableOperationsInMillions: Option[Double] = None,
    val replicateOnWrite:Option[Boolean] = None,
    val mergeShardsChance: Option[Double] = None,
    val keyValidationClass: Option[String] = None,
    val rowCacheProvider: Option[String ] = Option("org.apache.cassandra.cache.ConcurrentLinkedHashCacheProvider"),
    val keyAlias:Option[Array[Byte]] = None)

object FamilyDefinition {
  def apply(fdef:CfDef) = {
    new FamilyDefinition(
      name = fdef.getName,
      columnType = fdef.getColumn_type,
      comparatorType = Option(fdef.getComparator_type),
      subcomparatorType = Option(fdef.getSubcomparator_type),
      comment = Option(fdef.getComment),
      rowCacheSize = Option(fdef.getRow_cache_size),
      keyCacheSize = Option(fdef.getKey_cache_size),
      readRepairChance = Option(fdef.getRead_repair_chance),
      columnMetadata = Option(fdef.getColumn_metadata.map(c=>ColumnDefinition(c)).toList),
      gcGraceSeconds = Option(fdef.getGc_grace_seconds),
      defaultValidationClass = Option(fdef.getDefault_validation_class),
      id = Option(fdef.getId),
      minCompactionThreshold = Option(fdef.getMin_compaction_threshold),
      maxCompactionThreshold = Option(fdef.getMax_compaction_threshold),
      rowCacheSavePeriodInSeconds = Option(fdef.getRow_cache_save_period_in_seconds),
      keyCacheSavePeriodInSeconds = Option(fdef.getKey_cache_save_period_in_seconds),
      memtableFlushAfterMins = Option(fdef.getMemtable_flush_after_mins),
      memtableThroughputInMb = Option(fdef.getMemtable_throughput_in_mb),
      memtableOperationsInMillions = Option(fdef.getMemtable_operations_in_millions),
      mergeShardsChance = Option(fdef.getMerge_shards_chance),
      keyValidationClass = Option(fdef.getKey_validation_class),
      rowCacheProvider = Option(fdef.getRow_cache_provider),
      keyAlias = Option(fdef.getKey_alias))
  }
}
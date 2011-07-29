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

import results.{ResultSet, RowType, Row}
import schema._
import schema.Consistency._
import serializer.Serializers

import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.{TFramedTransport, TSocket}
import collection.JavaConversions._

import java.nio.ByteBuffer
import org.slf4j.LoggerFactory

import java.util.{Date, Iterator}
import system.{TokenRange, FamilyDefinition, KeyspaceDefinition}
import org.apache.cassandra.thrift.{CounterColumn, Compression, CfDef, KsDef, NotFoundException, ConsistencyLevel, Cassandra, Column => CassandraColumn, ColumnPath => CassandraColumnPath, ColumnParent => CassandraColumnParent, SliceRange => CassandraSliceRange, SlicePredicate => CassandraSlicePredicate, ColumnOrSuperColumn => CassandraColumnOrSuperColumn, KeyRange => CassandraKeyRange, TokenRange => CassandraTokenRange}

/**
 * A session connection to a cassandra instance.  This is really the heart of the api. It
 * uses the Cassandra Thrift api to access the database directly.  In most cases it maps
 * the classes in the schema package to classes in the thrift api.  Instances are created
 * by the SessionManager class, and Initialized with default values.  Socket connections
 * to the datastore are lazily initialized and are keep open until session.close is called.
 * <p>
 * Thrift api reference:
 * https://github.com/apache/cassandra/blob/cassandra-0.8/interface/cassandra.thrift
 *
 * @param host the host to connect too.
 * @param ksDef The KeySpace Definition used as a reference to map requests to the data store.
 * @param defaultConsistency The Default consistency to use when connecting to the data store.  It
 * 				defaults to Consistency.ONE.
 *
 * @author Michael Fortin
 */
class Session(host: Host, val ksDef: KeyspaceDefinition, val defaultConsistency: Consistency = Consistency.ONE) {
	private[this] val log = LoggerFactory.getLogger(classOf[Session])
  private[this] var openSock = false
  private[this] lazy val sock = new TFramedTransport(new TSocket(host.address, host.port, host.timeout))

  private[this] lazy val client = {
    val protocol = new TBinaryProtocol(sock)
    val c = new Cassandra.Client(protocol, protocol)
    sock.open()
    c.set_keyspace(ksDef.name)
    openSock = true
    c
  }

  private[this] implicit def toConsistencyLevel(c: Consistency) = c match {
    case ANY => ConsistencyLevel.ANY
    case ONE => ConsistencyLevel.ONE
    case QUORUM => ConsistencyLevel.QUORUM
    case EACH_QUORUM => ConsistencyLevel.EACH_QUORUM
    case LOCAL_QUORUM => ConsistencyLevel.LOCAL_QUORUM
    case Consistency.All => ConsistencyLevel.ALL
    case _ => sys.error("Unknown Level")
  }

  private[this] implicit def toColumnPath(c: ColumnPath) = {
    if (c.superColumn != null)
      new CassandraColumnPath(c.family).setSuper_column(c.superColumn).setColumn(c.column)
    else
      new CassandraColumnPath(c.family).setColumn(c.column)
  }

  private[this] implicit def toColumnParent(c: ColumnParent) = {
    val buffer = c.superColumn
    new CassandraColumnParent(c.family).setSuper_column(buffer)
  }

  private[this] implicit def toKeyRange(kr: KeyRange[_, _]) = {
    new CassandraKeyRange().setStart_key(kr.startBytes).setEnd_key(kr.finishBytes).setCount(kr.count)
  }

  private[this] implicit def toColumn(c: Column[_, _]) = {
    val column = new CassandraColumn()
    column.setName(c.nameBytes)
    column.setValue(c.valueBytes)
    column.setTimestamp(c.timestamp.getTime)
    column
  }

  private[this] implicit def toSlicePredicate(sp: SlicePredicate[_]) = {
    new CassandraSlicePredicate().setColumn_names(sp.toByteList)
  }

  private[this] implicit def toSlicePredicate(sp: SliceRange[_]) = {
    val r = new CassandraSliceRange(sp.startBytes, sp.finishBytes, false, sp.count)
    new CassandraSlicePredicate().setSlice_range(r)
  }

  private[this] def keyFor(c: Column[_, _]) = c.parent match {
    case s: StandardKey[_] => s.keyBytes
    case s: SuperColumn[_] => s.parent.keyBytes
  }

  private def toRows(cos: CassandraColumnOrSuperColumn, familyName:String, key:ByteBuffer):List[Row] = {
    if (cos == null) {
      List.empty[Row]
    }
    else if (cos.column != null || cos.getCounter_column != null) {
      val name = ByteBuffer.wrap(cos.column.getName)
      val value = ByteBuffer.wrap(cos.column.getValue)
      val timestamp = new Date(cos.column.getTimestamp)
      val rowType = if(cos.column != null) RowType.Standard else RowType.StandardCounter
      List(Row(rowType, familyName, key, null, name, value, timestamp))
    }
    else if (cos.getSuper_column != null || cos.getCounter_super_column != null) {
      val rType = if(cos.getSuper_column != null) RowType.Super else RowType.SuperCounter
			val sCol = cos.getSuper_column
      val sColName = ByteBuffer.wrap(sCol.getName)
      sCol.getColumns.map(c=>{
        val name = ByteBuffer.wrap(c.getName)
        val value = ByteBuffer.wrap(c.getValue)
        Row(rType,familyName,key,sColName, name, value, new Date(c.getTimestamp))
      }).toList
		}
    else {
      sys.error("Unknown column return type: '"+cos+"'" )
    }
  }

  private[this] implicit def toKsDef(c: KeyspaceDefinition) = {
    val d = new KsDef()
    d.setName(c.name)
    d.setStrategy_class(c.strategyClass)
    if (c.strategyOptions.isDefined)
      d.setStrategy_options(c.strategyOptions.get)
    d.setCf_defs(c.families.map(c=>toCfDef(c)))
    d.setDurable_writes(c.durableWrites)
    d
  }

  private[this] implicit def toCfDef(f: FamilyDefinition) = {
    val d = new CfDef()
    d.setName(f.name)
    d.setComparator_type(f.comparatorType.get)
    if (f.subcomparatorType.isDefined) d.setSubcomparator_type(f.subcomparatorType.get)
    if (f.comment.isDefined) d.setComment(f.comment.get)
    if (f.rowCacheSize.isDefined) d.setRow_cache_size(f.rowCacheSize.get)
    if (f.keyCacheSize.isDefined) d.setKey_cache_size(f.keyCacheSize.get)
    if (f.readRepairChance.isDefined) d.setRead_repair_chance(f.readRepairChance.get)
    if (f.columnMetadata.isDefined) d.setColumn_metadata(f.columnMetadata.get.map(_.asColumnDef))
    if (f.gcGraceSeconds.isDefined) d.setGc_grace_seconds(f.gcGraceSeconds.get)
    if (f.defaultValidationClass.isDefined) d.setDefault_validation_class(f.defaultValidationClass.get)
    if (f.id.isDefined) d.setId(f.id.get)
    if (f.minCompactionThreshold.isDefined) d.setMin_compaction_threshold(f.minCompactionThreshold.get)
    if (f.maxCompactionThreshold.isDefined) d.setMax_compaction_threshold(f.maxCompactionThreshold.get)
    if (f.rowCacheSavePeriodInSeconds.isDefined) d.setRow_cache_save_period_in_seconds(f.rowCacheSavePeriodInSeconds.get)
    if (f.keyCacheSavePeriodInSeconds.isDefined) d.setKey_cache_save_period_in_seconds(f.keyCacheSavePeriodInSeconds.get)
    if (f.memtableFlushAfterMins.isDefined) d.setMemtable_flush_after_mins(f.memtableFlushAfterMins.get)
    if (f.memtableThroughputInMb.isDefined) d.setMemtable_throughput_in_mb(f.memtableThroughputInMb.get)
    if (f.memtableOperationsInMillions.isDefined) d.setMemtable_operations_in_millions(f.memtableOperationsInMillions.get)
    if (f.replicateOnWrite.isDefined) d.setReplicate_on_write(f.replicateOnWrite.get)
    if (f.mergeShardsChance.isDefined) d.setMerge_shards_chance(f.mergeShardsChance.get)
    if (f.keyValidationClass.isDefined) d.setKey_validation_class(f.keyValidationClass.get)
    if (f.rowCacheProvider.isDefined) d.setRow_cache_provider(f.rowCacheProvider.get)
    if (f.keyAlias.isDefined) d.setKey_alias(f.keyAlias.get)
    d
  }

  private[this] implicit def toTokenRange(f: TokenRange) = {
    val d = new CassandraTokenRange()
    d.setStart_token(f.startToken)
    d.setEnd_token(f.endToken)
    d.setEndpoints(f.endpoints)
    d
  }

	/**
	 * socket connections are lazily and implicitly opened, but must be explicitly closed.  To end
   * a session this must be called.
	 */
  def close() {
    if (openSock)
      sock.close()
  }

	/**
	 * get the value of the column.  This assumes the input column does not have a value, this will
	 * return a results.Column with the name and value
   */
  def get(column: Column[_, _]): Option[Row] = get(column, defaultConsistency)

  /**
   * Increments a counter column.
   */
  def add(column: Column[_, _], level: Consistency = defaultConsistency) {
    log.debug("insert: {}",column)
    val counter = new CounterColumn()
    counter.setName(column.nameBytes)
    counter.setValue(column.value.asInstanceOf[Long])
    client.add(keyFor(column), column.columnParent, counter, level)
  }

  /**
   * Read the value of a single column, with the given consistency.
   *
   * @return An Option ColumnOrSuperColumn on success or None
   */
  def get(column: Column[_, _], level: Consistency): Option[Row] = {
    try {
      val columnOrSuperColumn = client.get(keyFor(column), column.columnPath, level)
      val list = toRows(columnOrSuperColumn, column.parent.family.name, column.parent.keyBytes)

      if (list.isEmpty)
        Option(null)
      else
        Option(list.last)
    }
    catch {
      case e: NotFoundException =>
        log.warn("NotFoundException caught, None returned")
        Option(null)
    }
  }

  /**
   * Set the value on an single Column
   */
  def insert(column: Column[_, _], level: Consistency = defaultConsistency) {
    log.debug("insert: {}",column)
    client.insert(keyFor(column), column.columnParent, column, level)
  }

	/**
	 * Remove a column and it's value.
   */
  def remove(column: Column[_, _]) {
    remove(keyFor(column), column.columnPath, new Date().getTime, defaultConsistency)
  }

	/**
	 * Remove a key and all it's child columns by using the default consistency level.
   */
	def remove(key: StandardKey[_]) {
    remove(key.keyBytes, ColumnPath(key.family.name,null,null), new Date().getTime, defaultConsistency)
  }

	/**
	 * Removes a key by the path and timestamp with the given consistency level.
   */
  def remove(k: ByteBuffer, path: ColumnPath, timestamp: Long, level: Consistency) {
    client.remove(k, path, timestamp, level)
  }

	/**
	 * get the count of the number of columns for a key
   */
  def count(key: Key, level: Consistency = defaultConsistency): Long = {
    val columnParent = new CassandraColumnParent(key.family.name)
    val sliceRange = new CassandraSliceRange(ByteBuffer.wrap("".getBytes), ByteBuffer.wrap("".getBytes), false, 32) // TODO fix max 32
    val predicate = new CassandraSlicePredicate().setSlice_range(sliceRange)
    client.get_count(key.keyBytes, columnParent, predicate, level)
  }

	/**
	 * List all the columns under the given key.
   */
	def list(key: StandardKey[_]): ResultSet = {
    sliceRange(key.sliceRange("","",false,100),defaultConsistency)
  }

  /**
   * List all the columns under the given super column.
   */
  def list(sc: SuperColumn[_]): ResultSet = {
    sliceRange(sc.sliceRange("","",false,100),defaultConsistency)
  }

  /**
   * List all the super columns and columns under the key
   */
  def list(sc: SuperKey[_]): ResultSet = {
    sliceRange(sc.sliceRange("","",false,100),defaultConsistency)
  }

	/**
	 * List the columns by slice predicate.  This uses the default consistency.
   */
  def slice(predicate: SlicePredicate[_]): ResultSet = {
    slice(predicate, defaultConsistency)
  }

	/**
	 * List all the columns by slice predicate and consistency level.
   */
  def slice(predicate: SlicePredicate[_], level: Consistency): ResultSet = {
    val results = client.get_slice(predicate.key.keyBytes, predicate.columnParent, predicate, level)
    val family = predicate.columnParent.family
    val key = predicate.key.keyBytes
    ResultSet(results.flatMap(sc => { toRows(sc, family, key)}).toList)
  }

	/**
	 * List all the columns by slice range. This uses the default consistency.
   */
  def sliceRange(range: SliceRange[_]): ResultSet = {
    sliceRange(range, defaultConsistency)
  }

	/**
	 * List all the columns by slice range and Consistency Level. This uses the default consistency.
   */
  def sliceRange(range: SliceRange[_], level: Consistency): ResultSet = {
    log.debug("range slice: " + range)
    val results = client.get_slice(range.keyBytes, range.columnParent, range, level)
    val key = range.key.keyBytes
    val family = range.columnParent.family
    ResultSet(results.flatMap(sc => { toRows(sc, family, key)}).toList)
  }

  /**
   *  Scroll through large slices by grabbing them form the datastore in chunks.  This will
   *  iterate over all elements including the start and end columns.
   */
  def scrollSliceRange[T](slice: SliceRange[T])(implicit m:Manifest[T]):Iterator[Row] = {
    new Iterator[Row] {
      private[this] var partial = sliceRange(slice).asInstanceOf[List[Row]]
      private[this] var index = 0

      def hasNext:Boolean = {
        if(partial.size > 0 && index == partial.size) {
          import results.RowType._
          val lastRow = partial.last
          val partialLast:Array[Byte] = lastRow.rowType match {
            case Standard => lastRow.column.array()
            case StandardCounter => lastRow.column.array()
            case Super => lastRow.superColumn.array()
            case SuperCounter => lastRow.superColumn.array()
          }
          val sliceLast = slice.finishBytes.array

          if(!java.util.Arrays.equals(partialLast,sliceLast)) {
            partial = {
              val pStart = Serializers.fromClassBytes(m.erasure,partialLast)
              val pFin = Serializers.fromClassBytes(m.erasure,sliceLast)
              val sliceCopy = slice.copy(start = pStart, finish = pFin)
              sliceRange(sliceCopy).asInstanceOf[List[Row]]
            }
            index = 1 // skip the first, because the slice is inclusive
          }
        }
        index < partial.size
      }

      def next:Row = {
        index = index + 1
        partial(index -1)
      }

      def remove(){}
    }
  }


  /**
   * Queries the data store by returning the key range inclusively.
   */
  def keyRange(range: KeyRange[_,_], level: Consistency = defaultConsistency): ResultSet = {
    val columnParent = range.columnParent
    val predicate = range.predicate
    val slice = client.get_range_slices(columnParent,predicate,range,level)
    val family = range.columnParent.family
    val list = slice.toList.flatMap(k=>{
      k.getColumns.toList.flatMap(c=>{
        val key = ByteBuffer.wrap(k.getKey)
        toRows(c, family, key)})
    })
    ResultSet(list)
  }

	/**
	 * List all the columns by Key range using the default consistency.
   */
  def keyRange[T <: AnyRef, C <: AnyRef](range: KeyRange[T, C]): ResultSet = {
    val results = client.get_range_slices(range.columnParent, range.predicate, range, defaultConsistency)
    val list = results.toList.flatMap(keyslice => {
      val key = ByteBuffer.wrap(keyslice.getKey)
      val family = range.columnParent.family
      keyslice.getColumns.toList.flatMap(c => { toRows(c, family, key)})
    })
    ResultSet(list)
  }

  def query(query:String,compression:String = ""):ResultSet = {
    val result = client.execute_cql_query(ByteBuffer.wrap(query.getBytes),Compression.NONE)
  // todo finish me
//    if (result.rows.size() > 0)
//      ResultSet(result.rows.map(r=>toRows()).toList)
//    else
      ResultSet(List.empty[Row])
  }


  def addKeyspace(keyspace:KeyspaceDefinition){
    client.system_add_keyspace(keyspace)
  }

  def updateKeyspace(keyspace:KeyspaceDefinition){
    client.system_update_keyspace(keyspace)
  }

  def dropKeyspace(keyspace:String){
    client.system_drop_keyspace(keyspace)
  }

  def addColumnFamily(family:FamilyDefinition){
    client.system_add_column_family(family)
  }
  def updateColumnFamily(family:FamilyDefinition){
    client.system_update_column_family(family)
  }

  def dropColumnFamily(family:String){
    client.system_drop_column_family(family)
  }

  def describeKeyspace(name:String) = KeyspaceDefinition(client.describe_keyspace(name))

  def describeKeyspaces = client.describe_keyspaces().map(ksDef => {
    KeyspaceDefinition(ksDef)
  }).toList

  def describeClusterName = client.describe_cluster_name()

  def describeVersion = client.describe_version()

  def describeRing(keyspace:String) = client.describe_ring(keyspace).map(t=>TokenRange(t))


}
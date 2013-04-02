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
import collection.Iterator

import java.nio.ByteBuffer
import org.slf4j.LoggerFactory

import java.util.Date
import system.{TokenRange, FamilyDefinition, KeyspaceDefinition}

import org.apache.cassandra.thrift.{CounterColumn, Compression, NotFoundException, ConsistencyLevel}
import org.apache.cassandra.thrift.{Cassandra, Column => CassandraColumn, ColumnPath => CassandraColumnPath, ColumnParent => CassandraColumnParent}
import org.apache.cassandra.thrift.{SliceRange => CassandraSliceRange, SlicePredicate => CassandraSlicePredicate}
import org.apache.cassandra.thrift.{ColumnOrSuperColumn => CassandraColumnOrSuperColumn, KeyRange => CassandraKeyRange, TokenRange => CassandraTokenRange}

import scala.language.{implicitConversions,reflectiveCalls}

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
 *                           defaults to Consistency.ONE.
 *
 * @author Michael Fortin
 */
class SessionImpl(host: Host, val ksDef: KeyspaceDefinition, val defaultConsistency: Consistency = Consistency.ONE) extends Session {
  private[this] val log = LoggerFactory.getLogger(classOf[SessionImpl])
  private[this] var openSock = false
  private[this] lazy val sock = new TFramedTransport(new TSocket(host.address, host.port, host.timeout))

  private[this] lazy val client = {
    val protocol = new TBinaryProtocol(sock)
    val c = new Cassandra.Client(protocol, protocol)
    sock.open()
    c.set_keyspace(ksDef.name)
    c
  }

  private[this] implicit def toConsistencyLevel(c: Consistency) = c match {
    case ANY => ConsistencyLevel.ANY
    case ONE => ConsistencyLevel.ONE
    case QUORUM => ConsistencyLevel.QUORUM
    case EACH_QUORUM => ConsistencyLevel.EACH_QUORUM
    case LOCAL_QUORUM => ConsistencyLevel.LOCAL_QUORUM
    case Consistency.All => ConsistencyLevel.ALL
    case _ => throw new RuntimeException("Unknown Consistency level: " + c.toString)
  }

  def closeAndMakeNewSession = {
    close()
    new SessionImpl(host, ksDef, defaultConsistency)
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

  private[this] implicit def toKeyRange(kr: KeyRange[_]) = {
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

  private[this] implicit def toSlicePredicate(sp: SliceRange) = {
    val r = new CassandraSliceRange().setStart(sp.startBytes)
            .setFinish(sp.finishBytes)
            .setReversed(sp.reverseList)
            .setCount(sp.max)
    //    val r = new CassandraSliceRange(sp.startBytes, sp.finishBytes, sp.reverse, sp.max)
    new CassandraSlicePredicate().setSlice_range(r)
  }

  private[this] def keyFor(c: {def parent: Key}) = c.parent match {
    case s: SuperKey[_] => s.keyBytes
    case s: SuperCounterKey[_] => s.keyBytes
    case s: StandardKey[_] => s.keyBytes
    case s: CounterKey[_] => s.keyBytes
    case s: SuperColumn[_] => s.parent.keyBytes
    case s: SuperCounterColumn[_] => s.parent.keyBytes
  }

  private def toRows(cos: CassandraColumnOrSuperColumn, familyName: String, key: ByteBuffer): List[Row] = {
    if (cos == null) {
      List.empty[Row]
    }
    else if (cos.getSuper_column != null) {
      val rType = RowType.Super
      val superColumnName = ByteBuffer.wrap(cos.getSuper_column.getName)
      cos.getSuper_column.getColumns.map(c => {
        val name = ByteBuffer.wrap(c.getName)
        val value = ByteBuffer.wrap(c.getValue)
        new Row(rType, familyName, key, superColumnName, name, value, new Date(c.getTimestamp))
      }).toList
    }
    else if (cos.getColumn != null) {
      val name = ByteBuffer.wrap(cos.getColumn.getName)
      val value = ByteBuffer.wrap(cos.getColumn.getValue)
      val timestamp = new Date(cos.getColumn.getTimestamp)
      val rowType = RowType.Standard
      List(new Row(rowType, familyName, key, null, name, value, timestamp))
    }
    else if (cos.getCounter_super_column != null) {
      val rType = RowType.SuperCounter
      val sCol = cos.getCounter_super_column
      val sColName = ByteBuffer.wrap(sCol.getName)
      sCol.getColumns.map(c => {
        val name = ByteBuffer.wrap(c.getName)
        val value = Serializers.toBytes(c.getValue)
        new Row(rType, familyName, key, sColName, name, value, null)
      }).toList
    }
    else if (cos.getCounter_column != null) {
      val name = ByteBuffer.wrap(cos.getCounter_column.getName)
      val value = Serializers.toBytes(cos.getCounter_column.getValue)
      val timestamp = null
      val rowType = RowType.StandardCounter
      List(new Row(rowType, familyName, key, null, name, value, timestamp))
    }
    else {
      throw new RuntimeException("Unknown column return type: '" + cos + "'")
    }
  }

  private[this] implicit def toTokenRange(f: TokenRange) = {
    val d = new CassandraTokenRange()
    d.setStart_token(f.startToken)
    d.setEnd_token(f.endToken)
    d.setEndpoints(f.endpoints)
    log.info("Add new TokenRange: " + d)
    d
  }

  /**
   * socket connections are lazily and implicitly opened, but must be explicitly closed.  To end
   * a session this must be called.
   */
  def close() {
    if (openSock)
      sock.close()
    openSock = false
  }

  /**
   * Increments a counter column.
   */
  def add(column: Column[_, _], level: Consistency = defaultConsistency) {
    log.trace("insert: {}", column)
    val counter = new CounterColumn()
    counter.setName(column.nameBytes)
    counter.setValue(column.value.asInstanceOf[Long])
    client.add(keyFor(column), column.columnParent, counter, level)
  }

  /**
   * get the value of the column.  This assumes the input column does not have a value, this will
   * return a results.Column with the name and value
   * Read the value of a single column, with the given consistency.
   *
   * @return An Option ColumnOrSuperColumn on success or None
   */
  def get(column: Column[_, _], level: Consistency): Option[Row] = {
    try {
      val columnOrSuperColumn = client.get(keyFor(column), toColumnPath(column.columnPath), level)
      val list = toRows(columnOrSuperColumn, column.parent.family.name, column.parent.keyBytes)

      if (list.isEmpty)
        Option(null)
      else
        Option(list.head)
    }
    catch {
      case e: NotFoundException =>
        log.warn("NotFoundException caught, None returned for column: {}", column)
        Option(null)
    }
  }

  /**
   * Set the value on an single Column
   */
  def insert(column: Column[_, _], level: Consistency = defaultConsistency) {
    client.insert(keyFor(column), column.columnParent, column, level)
  }

  /**
   * Remove a column and it's value.
   */
  def remove(column: ColumnName[_]) {
    remove(keyFor(column), column.columnPath, new Date().getTime, defaultConsistency)
  }

  /**
   * Remove a counter column and it's value.
   */
  def remove(column: CounterColumnName[_]) {
    remove(keyFor(column), column.columnPath, new Date().getTime, defaultConsistency)
  }

  /**
   * Remove a counter column and it's value.
   */
  def remove(column: SuperColumn[_]) {
    remove(keyFor(column), column.columnPath, new Date().getTime, defaultConsistency)
  }

  /**
   * Remove a column and it's value.
   */
  def remove(column: Column[_, _]) {
    remove(keyFor(column), column.columnPath, new Date().getTime, defaultConsistency)
  }

  def remove(key: Key) {
    key match {
      case s: StandardKey[_] =>
        remove(s.keyBytes, ColumnPath(s.family.name, null, null), new Date().getTime, defaultConsistency)
      case s: SuperKey[_] =>
        remove(s.keyBytes, ColumnPath(s.family.name, null, null), new Date().getTime, defaultConsistency)
      case s: CounterKey[_] =>
        client.remove_counter(s.keyBytes, ColumnPath(s.family.name, null, null), defaultConsistency)
      case s: SuperColumn[_] =>
        remove(s.keyBytes, ColumnPath(s.family.name, null, null), new Date().getTime, defaultConsistency)
      case s: SuperCounterColumn[_] =>
        client.remove_counter(s.keyBytes, ColumnPath(s.family.name, null, null), defaultConsistency)
      case s: SuperCounterKey[_] =>
        client.remove_counter(s.keyBytes, ColumnPath(s.family.name, null, null), defaultConsistency)
    }
  }

  /**
   * Removes a key by the path and timestamp with the given consistency level.
   */
  def remove(k: ByteBuffer, path: ColumnPath, timestamp: Long, level: Consistency) {
    client.remove(k, path, timestamp, level)
  }

  def remove(key: SuperCounterColumn[_]) {
    client.remove_counter(key.keyBytes, ColumnPath(key.family.name, null, null), defaultConsistency)
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
   * List all the columns by slice predicate and consistency level.
   */
  def slice(predicate: SlicePredicate[_], level: Consistency): Seq[Row] = {
    val results = client.get_slice(predicate.key.keyBytes, predicate.columnParent, predicate, level)
    val family = predicate.columnParent.family
    val key = predicate.key.keyBytes
    results.flatMap(sc => { toRows(sc, family, key)}).toSeq
  }

  /**
   * List all the columns by slice range and Consistency Level. This uses the default consistency.
   */
  def sliceRange(range: SliceRange, level: Consistency): Seq[Row] = {
    val results = client.get_slice(range.keyBytes, range.columnParent, range, level)
    val family = range.columnParent.family
    results.flatMap(sc => { toRows(sc, family, range.keyBytes)}).toSeq
  }

  /**
   * Scroll through large slices by grabbing them form the data store in chunks.  This will
   * iterate over all elements including the start and end columns.
   *
   * @param initSliceRange the slice range to iterate over.
   */
  def scrollSliceRange(initSliceRange: SliceRange): Iterator[Row] = {
    new Iterator[Row] {
      private[this] var partial = sliceRange(initSliceRange)
      private[this] var index = 0

      def hasNext: Boolean = {
        if (partial.size > 0 && index == partial.size) {
          import results.RowType._
          val lastRow = partial.last
          val partialLast: ByteBuffer = lastRow.rowType match {
            case Standard => lastRow.column
            case StandardCounter => lastRow.column
            case Super => lastRow.superColumn
            case SuperCounter => lastRow.superColumn
          }
          val sliceLast: ByteBuffer = ByteBuffer.wrap(initSliceRange.finishBytes)

          // if doing a slice of the whole table, using empty arrays as the start and finish
          // of the slice, then this will never be false.  the
          if (partialLast.compareTo(sliceLast) != 0) {
            partial = {
              val sliceCopy = initSliceRange.copy(start = partialLast.array(), finish = sliceLast.array())
              sliceRange(sliceCopy)
            }
            index = 1 // skip the first, because the slice is inclusive
          }
        }
        index < partial.size
      }

      def next(): Row = {
        index = index + 1
        partial(index - 1)
      }
    }
  }


  /**
   * Queries the data store by returning the key range inclusively.
   */
  def keyRange(range: KeyRange[_], level: Consistency = defaultConsistency): Seq[Row] = {
    val columnParent = range.columnParent
    val predicate = range.predicate.getOrElse(new SlicePredicate(Array.empty[Byte], null))
    val slice = client.get_range_slices(columnParent, predicate, range, level)
    val family = range.columnParent.family
    slice.flatMap(k => {
      k.getColumns.flatMap(c => {
        val key = ByteBuffer.wrap(k.getKey)
        toRows(c, family, key)
      })
    }).toSeq
  }

  def query(query: String, compression: String = ""): Seq[Row] = {
    val result = client.execute_cql_query(ByteBuffer.wrap(query.getBytes), Compression.NONE)
    result.getRows.flatMap(cqlRow => fromCqlRow(cqlRow.getColumns, cqlRow.getKey)).toSeq
  }

  def fromCqlRow(columns:java.util.List[CassandraColumn], key:Array[Byte]) = {
    columns.map(c=>{
      new Row(RowType.Cql, null, ByteBuffer.wrap(key), null, ByteBuffer.wrap(c.getName), ByteBuffer.wrap(c.getValue), new Date(c.getTimestamp))
    })
  }

  def addKeySpace(ks: KeyspaceDefinition) {
    log.info("add keyspace: {}", ks)
    val protocol = new TBinaryProtocol(sock)
    val c = new Cassandra.Client(protocol, protocol)
    sock.open()
    c.system_add_keyspace(ks.toKsDef)
    sock.close()
  }

  def updateKeySpace(keyspace: KeyspaceDefinition) {
    log.info("update keyspace: {}", keyspace)
    val protocol = new TBinaryProtocol(sock)
    val c = new Cassandra.Client(protocol, protocol)
    client.system_update_keyspace(keyspace.toKsDef)
    sock.close()
  }

  def dropKeySpace(keyspace: String) {
    log.info("drop keyspace: {}", keyspace)
    val protocol = new TBinaryProtocol(sock)
    val c = new Cassandra.Client(protocol, protocol)
    client.system_drop_keyspace(keyspace)
    sock.close()
  }

  def addColumnFamily(family: FamilyDefinition) {
    log.info("add columnFamily: {}", family)
    client.system_add_column_family(family.asCfDef)
  }

  def updateColumnFamily(family: FamilyDefinition) {
    log.info("update columnFamily: {}", family)
    client.system_update_column_family(family.asCfDef)
  }

  def dropColumnFamily(family: String) {
    log.info("drop columnFamily: {}", family)
    client.system_drop_column_family(family)
  }

  def describeKeySpace(name: String) = {
    KeyspaceDefinition(client.describe_keyspace(name))
  }

  def describeKeySpaces = {
    client.describe_keyspaces().map(ksDef => {
      KeyspaceDefinition(ksDef)
    }).toList
  }

  def describeClusterName = {
    client.describe_cluster_name()
  }

  def describeVersion = {
    client.describe_version()
  }

  def describeRing(keyspace: String) = {
    client.describe_ring(keyspace).map(t => TokenRange(t)).toList
  }


}
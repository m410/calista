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

import schema._
import schema.Consistency._
import results.{Column => RColumn, SuperColumn => RSuperColumn}

import java.util.Date
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.{TFramedTransport, TSocket}
import collection.JavaConversions._

import org.apache.cassandra.thrift.{NotFoundException, ConsistencyLevel}
import org.apache.cassandra.thrift.{Cassandra, Column => CassandraColumn}
import org.apache.cassandra.thrift.{ColumnPath => CassandraColumnPath}
import org.apache.cassandra.thrift.{ColumnParent => CassandraColumnParent}
import org.apache.cassandra.thrift.{SliceRange => CassandraSliceRange}
import org.apache.cassandra.thrift.{SlicePredicate => CassandraSlicePredicate}
import org.apache.cassandra.thrift.{ColumnOrSuperColumn => CassandraColumnOrSuperColumn}
import org.apache.cassandra.thrift.{KeyRange => CassandraKeyRange}


import java.nio.ByteBuffer
import collection.mutable.HashMap
import serializer.{UUIDSerializer, Serializers}
import org.slf4j.LoggerFactory

/**
 * A session connection to a cassandra instance.  
 *
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
    case _ => error("Unknown Level")
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
    new CassandraColumn(c.nameBytes, c.valueBytes, c.timestamp.getTime)
  }

  private[this] implicit def toSlicePredicate(sp: SlicePredicate[_]) = {
    new CassandraSlicePredicate().setColumn_names(sp.toByteList)
  }

  private[this] implicit def toSlicePredicate(sp: SliceRange[_]) = {
    val r = new CassandraSliceRange(sp.startBytes, sp.finishBytes, false, sp.count)
    new CassandraSlicePredicate().setSlice_range(r)
  }

  private[this] def fromColumnOrSuperColumn(cos: CassandraColumnOrSuperColumn) = {
    if (cos == null)
      null
    if (cos.column != null)
      RColumn(cos.column.getName, cos.column.getValue, new Date(cos.column.getTimestamp))
    else
      RSuperColumn(cos.super_column.name, null, cos.super_column.columns.map(c => {
        RColumn(c.getName, c.getValue, new Date(c.getTimestamp))
      }).toList)
  }

  private[this] def keyFor(c: Column[_, _]) = c.parent match {
    case s: StandardKey[_] => s.keyBytes
    case s: SuperColumn[_] => s.superKey.keyBytes
  }

	/**
	 * socket connections are lazily and implicitly opened, but must be explicitly closed.
	 */
  def close = {
    if (openSock)
      sock.close()
  }

	/**
	 * get the value of the column.  This assumes the input column does not have a value, this will
	 * return a results.Column with the name and value
   */
  def get(column: Column[_, _]): Option[ColumnOrSuperColumn] = get(column, defaultConsistency)

  /**
   * Read the value of a single column, with the given consistency
   */
  def get(column: Column[_, _], level: Consistency): Option[ColumnOrSuperColumn] = {
    try {
      val columnOrSuperColumn = client.get(keyFor(column), column.columnPath, level)
      Option(fromColumnOrSuperColumn(columnOrSuperColumn))
    }
    catch {
      case e: NotFoundException =>
        println(e.getMessage)
        e.printStackTrace
        Option(null)
    }
  }

	/**
	 * List all the columns under the given key.
   */
	def list(key: StandardKey[_]): List[ColumnOrSuperColumn] = {
    import schema.Conversions._
    val slice = key \("","")
    list(slice,defaultConsistency)
  }

	/**
	 * List the columns by slice predicate
   */
  def list(predicate: SlicePredicate[_]): List[ColumnOrSuperColumn] = {
    list(predicate, defaultConsistency)
  }

	/**
	 * List all the columns by slice predicate and consistency level.
   */
  def list(predicate: SlicePredicate[_], level: Consistency): List[ColumnOrSuperColumn] = {
    val results = client.get_slice(predicate.key.keyBytes, predicate.columnParent, predicate, level)
    results.map(sc => fromColumnOrSuperColumn(sc)).toList
  }

	/**
	 * List all the columns by slice range.
   */
  def list(range: SliceRange[_]): List[ColumnOrSuperColumn] = {
    list(range, defaultConsistency)
  }

	/**
	 * List all the columns by slice range and Consistency Level.
   */
  def list(range: SliceRange[_], level: Consistency): List[ColumnOrSuperColumn] = {
    val results = client.get_slice(range.key.keyBytes, range.columnParent, range, level)
    results.map(sc => fromColumnOrSuperColumn(sc)).toList
  }

	/**
	 * List all the columns by Key range.
   */
  def list[T <: AnyRef, C <: AnyRef](range: KeyRange[T, C]): Map[T, List[ColumnOrSuperColumn]] = {
    val results = client.get_range_slices(range.columnParent, range.predicate, range, defaultConsistency)
    val map = HashMap[T, List[ColumnOrSuperColumn]]()
    results.foreach(keyslice => {
      val tKey = Serializers.fromBytes(range.start,ByteBuffer.wrap(keyslice.getKey))
      val columnList = keyslice.getColumns.map(c => fromColumnOrSuperColumn(c)).toList
      map += tKey -> columnList
    })
    map.toMap
  }

  /**
   * Set the value on an single Column
   */
  def insert(column: Column[_, _], level: Consistency = defaultConsistency) = {
    client.insert(keyFor(column), column.columnParent, column, level)
  }

	/**
	 * Remove a column and it's value.
   */
  def remove(column: Column[_, _]): Unit = {
    remove(keyFor(column), column.columnPath, new Date().getTime, defaultConsistency)
  }

	/**
	 * Remove a key and all it's child columns.
   */
	def remove(key: StandardKey[_]): Unit = {
    remove(key.keyBytes, ColumnPath(key.family.name,null,null), new Date().getTime, defaultConsistency)
  }

	/**
	 *
   */
  def remove(k: ByteBuffer, path: ColumnPath, timestamp: Long, level: Consistency): Unit = {
    client.remove(k, path, timestamp, level)
  }
	
	/**
	 * get the count of the number of columns for a key
   */
  def count(key: Key, max: Int = 100, level: Consistency = defaultConsistency): Long = {
    val columnParent = new CassandraColumnParent(key.family.name)
    val sliceRange = new CassandraSliceRange(ByteBuffer.wrap("".getBytes), ByteBuffer.wrap("".getBytes), false, max)
    val predicate = new CassandraSlicePredicate().setSlice_range(sliceRange)
    client.get_count(key.keyBytes, columnParent, predicate, level)
  }

	/**
	 * Batch insert
	 */
  def batch(mutations:List[Mutation], level: Consistency = defaultConsistency) = {
      
  }
}
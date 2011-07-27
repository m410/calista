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

import results.{ResultSet, RowType, Row, KeySlice, Column => RColumn, SuperColumn => RSuperColumn}
import schema._
import schema.Consistency._
import serializer.Serializers

import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.{TFramedTransport, TSocket}
import collection.JavaConversions._
import collection.mutable.HashMap

import java.nio.ByteBuffer
import org.slf4j.LoggerFactory

import org.apache.cassandra.thrift.{NotFoundException, ConsistencyLevel, Cassandra, Column => CassandraColumn, ColumnPath => CassandraColumnPath, ColumnParent => CassandraColumnParent, SliceRange => CassandraSliceRange, SlicePredicate => CassandraSlicePredicate, ColumnOrSuperColumn => CassandraColumnOrSuperColumn, KeyRange => CassandraKeyRange}
import java.util.{Date, Iterator}
import sun.jvm.hotspot.debugger.posix.elf.ELFSectionHeader

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
 * @param ksDef The KeySpace Defininition used as a reference to map requests to the data store.
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

  @deprecated
  private[this] def fromColumnOrSuperColumn(cos: CassandraColumnOrSuperColumn) = {
    if (cos == null)
      null
    else if (cos.column != null)
      RColumn(cos.column.getName, cos.column.getValue, new Date(cos.column.getTimestamp))
    else {
			val sCol = cos.getSuper_column
			val cols = sCol.getColumns.map(c=>RColumn(c.getName, c.getValue, new Date(c.getTimestamp))).toList
			RSuperColumn(sCol.getName, null, cols)
		}
  }

  private[this] def keyFor(c: Column[_, _]) = c.parent match {
    case s: StandardKey[_] => s.keyBytes
    case s: SuperColumn[_] => s.parent.keyBytes
  }

  private def toRows(cos: CassandraColumnOrSuperColumn,query:Column[_,_]):List[Row] = {
    if (cos == null) {
      List.empty[Row]
    }
    else if (cos.column != null || cos.getCounter_column != null) {
      val familyName = query.parent.family.name
      val name = ByteBuffer.wrap(cos.column.getName)
      val value = ByteBuffer.wrap(cos.column.getValue)
      val timestamp = new Date(cos.column.getTimestamp)
      val key = query.parent.keyBytes
      val rowType = if(cos.column != null) RowType.Standard else RowType.StandardCounter
      List(Row(rowType, familyName, key, null, name, value, timestamp))
    }
    else if (cos.getSuper_column != null || cos.getCounter_super_column != null) {
      val rType = if(cos.getSuper_column != null) RowType.Super else RowType.SuperCounter
			val sCol = cos.getSuper_column
      val fName = query.parent.family.name
      val key = query.parent.keyBytes
      val sColName = ByteBuffer.wrap(sCol.getName)
      sCol.getColumns.map(c=>{
        val name = ByteBuffer.wrap(c.getName)
        val value = ByteBuffer.wrap(c.getValue)
        Row(rType,fName,key,sColName, name, value, new Date(c.getTimestamp))
      }).toList
		}
    else {
      sys.error("Unknown column return type: '"+cos+"'" )
    }
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
  def get(column: Column[_, _]): Option[ColumnOrSuperColumn] = get(column, defaultConsistency)

  def add(column: Column[_, _]): Row = null// todo STUBBED  

  /**
   * Read the value of a single column, with the given consistency.
   *
   * @return An Option ColumnOrSuperColumn on success or None
   */
  def get(column: Column[_, _], level: Consistency): Option[ColumnOrSuperColumn] = {
    try {
      val columnOrSuperColumn = client.get(keyFor(column), column.columnPath, level)
      Option(fromColumnOrSuperColumn(columnOrSuperColumn))
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
	def list(key: StandardKey[_]): List[ColumnOrSuperColumn] = {
    sliceRange(key.sliceRange("","",false,100),defaultConsistency)
  }

  /**
   * List all the columns under the given super column.
   */
  def list(sc: SuperColumn[_]): List[ColumnOrSuperColumn] = {
    sliceRange(sc.sliceRange("","",false,100),defaultConsistency)
  }

  /**
   * List all the super columns and columns under the key
   */
  def list(sc: SuperKey[_]): List[ColumnOrSuperColumn] = {
    sliceRange(sc.sliceRange("","",false,100),defaultConsistency)
  }

	/**
	 * List the columns by slice predicate.  This uses the default consistency.
   */
  def slice(predicate: SlicePredicate[_]): List[ColumnOrSuperColumn] = {
    slice(predicate, defaultConsistency)
  }

	/**
	 * List all the columns by slice predicate and consistency level.
   */
  def slice(predicate: SlicePredicate[_], level: Consistency): List[ColumnOrSuperColumn] = {
    val results = client.get_slice(predicate.key.keyBytes, predicate.columnParent, predicate, level)
    results.map(sc => fromColumnOrSuperColumn(sc)).toList
  }

	/**
	 * List all the columns by slice range. This uses the default consistency.
   */
  def sliceRange(range: SliceRange[_]): List[ColumnOrSuperColumn] = {
    sliceRange(range, defaultConsistency)
  }

	/**
	 * List all the columns by slice range and Consistency Level. This uses the default consistency.
   */
  def sliceRange(range: SliceRange[_], level: Consistency): List[ColumnOrSuperColumn] = {
    log.debug("range slice: " + range)
    val results = client.get_slice(range.keyBytes, range.columnParent, range, level)
    results.map(sc => fromColumnOrSuperColumn(sc)).toList
  }

  /**
   *  Scroll through large slices by grabbing them form the datastore in chunks.  This will
   *  iterate over all elements including the start and end columns.
   */
  def scrollSliceRange[T](slice: SliceRange[T])(implicit m:Manifest[T]):Iterator[ColumnOrSuperColumn] = {
    new Iterator[ColumnOrSuperColumn] {
      private[this] var partial = sliceRange(slice).asInstanceOf[List[ColumnOrSuperColumn]]
      private[this] var index = 0

      def hasNext:Boolean = {
        if(partial.size > 0 && index == partial.size) {
          val partialLast:Array[Byte] = partial.last match {
            case c:RColumn =>c.name
            case s:RSuperColumn[_]=>s.bytes.array
          }
          val sliceLast = slice.finishBytes.array

          if(!java.util.Arrays.equals(partialLast,sliceLast)) {
            partial = {
              val pStart = Serializers.fromClassBytes(m.erasure,partialLast)
              val pFin = Serializers.fromClassBytes(m.erasure,sliceLast)
              val sliceCopy = slice.copy(start = pStart, finish = pFin)
              sliceRange(sliceCopy).asInstanceOf[List[ColumnOrSuperColumn]]
            }
            index = 1 // skip the first, because the slice is inclusive
          }
        }
        index < partial.size
      }

      def next:ColumnOrSuperColumn = {
        index = index + 1
        partial(index -1)
      }

      def remove(){}
    }
  }


  /**
   * Queries the data store by returning the key range inclusively.
   */
  def keyRange(range: KeyRange[_,_], level: Consistency = defaultConsistency):List[KeySlice] = {
    val columnParent = range.columnParent
    val predicate = range.predicate
    val slice = client.get_range_slices(columnParent,predicate,range,level)
    slice.map(k=>KeySlice(k.getKey,k.getColumns.map(c=>fromColumnOrSuperColumn(c)).toList)).toList
  }

	/**
	 * List all the columns by Key range using the default consistency.
   */
  def keyRange[T <: AnyRef, C <: AnyRef](range: KeyRange[T, C]): Map[T, List[ColumnOrSuperColumn]] = {
    val results = client.get_range_slices(range.columnParent, range.predicate, range, defaultConsistency)
    val map = HashMap[T, List[ColumnOrSuperColumn]]()
    results.foreach(keyslice => {
      val tKey = Serializers.fromBytes(range.start,ByteBuffer.wrap(keyslice.getKey))
      val columnList = keyslice.getColumns.map(c => fromColumnOrSuperColumn(c)).toList
      map += tKey -> columnList
    })
    map.toMap
  }


  def query(query:String,compression:String = ""):ResultSet = {
    ResultSet(List.empty[Row])
  }

  def addKeyspace(keyspace:KeyspaceDefinition){}
  def updateKeyspace(keyspace:KeyspaceDefinition){}
  def dropKeyspace(keyspace:KeyspaceDefinition){}

  def addColumnFamily(family:FamilyDefinition){}
  def updateColumnFamily(family:FamilyDefinition){}
  def dropColumnFamily(family:FamilyDefinition){}

  def describeKeyspace(name:String):KeyspaceDefinition = null
  def describeKeyspaces():List[KeyspaceDefinition] = List.empty[KeyspaceDefinition]
  def describeClusterName():String = ""
  def describeRing(){}


}
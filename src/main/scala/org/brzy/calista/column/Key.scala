package org.brzy.calista.column

import java.nio.ByteBuffer

/**
 * Document Me..
 * 
 * @author Michael Fortin
 */
trait Key {
  def keyBytes:ByteBuffer
  def family:ColumnFamily
  def columnPath:ColumnPath
}
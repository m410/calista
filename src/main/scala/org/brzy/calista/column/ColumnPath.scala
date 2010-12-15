package org.brzy.calista.column

import java.nio.ByteBuffer

/**
 * Document Me..
 *
 * @author Michael Fortin
 */
case class ColumnPath(family: String, superColumn: ByteBuffer, column: ByteBuffer)
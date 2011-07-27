package org.brzy.calista.schema

/**
 * Thrown by the ColumnFamily if the family does not exist.
 * 
 * @author Michael Fortin
 */
class UnknownFamilyException(msg:String) extends RuntimeException(msg)
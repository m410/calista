package org.brzy.calista.schema

/**
 * Thrown when trying to get the value of a counter column but the column doesn't exist.
 *
 * @author Michael Fortin
 */
class NoColumnException(msg: String) extends RuntimeException(msg)
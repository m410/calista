package org.brzy.calista.schema

import java.lang.RuntimeException

/**
 * Thrown by the DslNode trait for unimplemented methods.
 * 
 * @author Michael Fortin
 */
class InvalidNodeUseException(msg:String) extends RuntimeException(msg)
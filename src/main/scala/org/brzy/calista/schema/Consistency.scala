package org.brzy.calista.schema

/**
 * Document Me..
 *
 * @author Michael Fortin
 */
object Consistency extends Enumeration {
  type Consistency = Value
  val ANY = Value("ANY")
  val ONE = Value("ONE")
  val QUORUM = Value("QUORUM")
  val EACH_QUORUM = Value("EACH_QUORUM")
  val LOCAL_QUORUM = Value("LOCAL_QUORUM") 
  val All = Value("ALL")
}
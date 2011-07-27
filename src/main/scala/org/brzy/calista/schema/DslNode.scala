package org.brzy.calista.schema

import java.util.Date
import org.brzy.calista.results.{Row, ResultSet}

/**
 * This is so different levels of the dsl access tree can be treated the same.  Many of these
 * methods are stubbed out in the implementation, none use all of them, but it's necessary
 * to be able to have a clean dsl.
 *
 * @author Michael Fortin
 */
trait DslNode {

  def nodePath: String

  /**
   * @returns can be a key, super key, a super column, or a column with name only.
   */
  def |[N: Manifest](name: N): DslNode =
    throw new InvalidNodeUseException(nodePath)

  /**
   * This is only implemented on keys and super columns.  This creates a column that is not
   * bound to the datastore.  It can be used to create many columns at once to be saved in
   * batch mode.
   *
   * @returns describes a full column
   */
  def |[N: Manifest, V: Manifest](key: N, value: V = null, timestamp: Date): Column[N, V] =
    throw new InvalidNodeUseException(nodePath)

  /**
   * SlicePredicate
   *
   * @returns a result set of rows
   */
  def \[N: Manifest](name: Array[N]): ResultSet =
    throw new InvalidNodeUseException(nodePath)

  /**
   * SliceRange, this can be called from a key or a super column, it returns a list or rows. It
   * does not apply to column families.
   *
   * @returns a result set of rows
   */
  def \\[N: Manifest](begin: N, end: N, reverse: Boolean, max: Int): ResultSet =
    throw new InvalidNodeUseException(nodePath)

  /**
   * called on counter column nodes only, this adds the value to the counter column name.
   */
  def +=(amount: Long) {
    throw new InvalidNodeUseException(nodePath)
  }

  /**
   * called on counter column nodes only, this subtracts the value to the counter column name.
   */
  def -=(amount: Long) {
    throw new InvalidNodeUseException(nodePath)
  }

  /**
   * Only applies to standard columns with names only.  This sets the value to
   * the column.
   */
  def <=[N: Manifest](value: N){
    throw new InvalidNodeUseException(nodePath)
  }

  def as[V:Manifest]:V = throw new InvalidNodeUseException(nodePath)

  def count:Long = throw new InvalidNodeUseException(nodePath)
}
package org.brzy.calista.dsl

import org.brzy.calista.results.{Row, ResultSet}
import java.util.Date
import org.brzy.calista.schema.{Column,SlicePredicate,SliceRange}

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
  def ||[N: Manifest, V: Manifest](key: N, value: V, timestamp: Date = new Date()): Column[N, V] =
    throw new InvalidNodeUseException(nodePath)

  /**
   * SlicePredicate
   *
   * @returns a result set of rows
   */
  def \[N: Manifest](predicates: N*): SlicePredicate[N] =
    throw new InvalidNodeUseException(nodePath)

  /**
   * SliceRange, this can be called from a key or a super column, it returns a list or rows. It
   * does not apply to column families.
   *
   * @returns a result set of rows
   */
  def \\[N: Manifest](begin: N, end: N, reverse: Boolean = false, max: Int = 100): SliceRange[N] =
    throw new InvalidNodeUseException(nodePath)

  /**
   * called on counter column name nodes only, this adds the value to the counter column name.
   */
  def +=(amount: Long) {
    throw new InvalidNodeUseException(nodePath)
  }

  /**
   * called on counter column name nodes only, this subtracts the value to the counter column name.
   */
  def -=(amount: Long) {
    throw new InvalidNodeUseException(nodePath)
  }

  /**
   * Only applies to standard or super column name nodes to retrieve the value
   */
  def valueAs[V:Manifest]:Option[V] = throw new InvalidNodeUseException(nodePath)

  /**
   * Only applies to counter column names to get the value of the counter
   */
  def count:Long = throw new InvalidNodeUseException(nodePath)

  /**
   * Applies to a column name node, super columns and keys.  This will remove the node from the
   * datastore immediately.
   */
  def remove() {throw new InvalidNodeUseException(nodePath)}

}
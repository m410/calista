package org.brzy.calista.column

/**
 * Document Me..
 *
 * @author Michael Fortin
 */
case class ColumnFamily(name: String) {
  def |[T](key: T)(implicit t: Manifest[T]) = StandardKey(key, this)

  def |^[T](key: T)(implicit t: Manifest[T]) = SuperKey(key, this)

  def \[T,C](start: T, end: T, columns:List[C], count: Int = 100) =
      KeyRange(start, end, SlicePredicate(columns, null), this, count)
}
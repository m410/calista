package org.brzy.calista.ocm

/**
 * Document Me..
 * 
 * @author Michael Fortin
 */
trait Dao[T,K] {
  def get(key:K):T
  def insert(t:T)
  def remove(t:T)

  def orm = Map.empty[String,String]
}
/*
 * Copyright 2010 Michael Fortin <mike@brzy.org>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");  you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed 
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR 
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */
package org.brzy.calista.schema

import org.brzy.calista.Calista
/**
 * Represents a column family to query in the database.  This is the entry point to DSL access
 * to the datastore.
 *
 * @author Michael Fortin
 */
trait ColumnFamily {

  def name:String


  def definition = try {
    Calista.value.ksDef.families.find(_.name == name) match {
      case Some(f) => f
      case _ => throw new UnknownFamilyException("No ColumnFamily with name: " + name)
    }
  }
  catch {
    case n:NullPointerException => throw new SessionScopeException("Out of session scope",n)
  }

  override def toString = "ColumnFamily("+name+")"


  //
//  def |[T:Manifest](k: T) = standardKey(k)
//
//  def ||[T:Manifest](k: T) = superKey(k)
//
////  /**
////   * Non DSL access to the key under the column family.  This will return either a standard key
////   * or a super key.  The type of key is discovered by the meta data on the datastore.
////   *
////   * @returns
////   * @throws UnknownFamilyException if the family name does not exist in the data store.
////   */
////  def key[T:Manifest](key: T):Either[StandardKey[T],SuperKey[T]] = {
////    val family = Calista.value.ksDef.families.find(_.name == name) match {
////      case Some(f) => f
////      case _ => throw new UnknownFamilyException("No ColumnFamily with name: '" + name +"'")
////    }
////
////    if (family.columnType == "Standard")
////      Left(StandardKey(key, this, family)) // pass along the column family definition
////    else if (family.columnType == "Counter")
////      Left(StandardKey(key, this, family))
////    else
////      Right(SuperKey(key,this, family))
////  }
//
//  def superKey[T:Manifest](key: T):SuperKey[T] = SuperKey(key,this, familyDef)
//
//  def standardKey[T:Manifest](key: T):StandardKey[T] = StandardKey(key,this, familyDef)
//
//  def keyRange[T,C:Manifest](start: T, end: T, columns:List[C], count: Int = 100) =
//			KeyRange(start, end, SlicePredicate(columns.toArray, null), this, count)
//
//  protected[schema] def familyDef = try {
//    Calista.value.ksDef.families.find(_.name == name) match {
//      case Some(f) => f
//      case _ => throw new UnknownFamilyException("No ColumnFamily with name: " + name)
//    }
//  }
//  catch {
//    case n:NullPointerException => throw new SessionScopeException("Out of session scope",n)
//  }
}

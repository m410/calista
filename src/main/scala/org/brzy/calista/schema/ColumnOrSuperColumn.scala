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

/**
 * Marker trait for Columns and Super Columns.  The only thing that columns and super columns
 * have in common is that they both have parent keys.
 * 
 * @author Michael Fortin
 * @version $Id: $
 */
trait ColumnOrSuperColumn {

  /**
   * The parent can be a Standard key, a Super Key or a Super Column.
   */
  // doesn't compile because results.Column & superColumn also exntend this.
//  def parent:Key
}
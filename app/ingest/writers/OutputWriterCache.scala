/*
 *
 *  * Copyright 2016 HM Revenue & Customs
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 */

package ingest.writers

import java.util.Date

import services.model.StateModel
import uk.co.hmrc.address.osgb.DbAddress

import scala.collection.mutable

// This exists primarily as an aid for analysis. It is not intended for performance enhancement.

class OutputWriterCache(peer: OutputWriter, max: Int) extends OutputWriter {
  private val cache = new mutable.HashMap[String, DbAddress]
  private val uprns = new mutable.Queue[String]()

  override def existingTargetThatIsNewerThan(date: Date): Option[String] =
    peer.existingTargetThatIsNewerThan(date)

  override def begin() {
    peer.begin()
  }

  override def output(a: DbAddress) {
    peer.output(a)
    cache.put(a.id, a)
    uprns.enqueue(a.id)
    if (uprns.size > max)    {
      val lost = uprns.dequeue()
      cache.remove(lost)
    }
  }

  override def end(completed: Boolean): StateModel = peer.end(completed)

  def get(id: String): Option[DbAddress] = cache.get(id)

  def apply(id: String): DbAddress = cache(id)

  def clear() {
    cache.clear()
    uprns.clear()
  }

  def size: Int = cache.size

  def isEmpty: Boolean = cache.isEmpty
}

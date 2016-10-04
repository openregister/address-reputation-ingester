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

package util

import scala.collection.mutable

/**
  * Holds a window of values, up to a specified maximum size. When the cache is full, oldest items
  * are evicted when new items are inserted.
  */
class FifoCache[K, V](max: Int, identifier: (V) => K) {

  private val cache = new mutable.HashMap[K, V]
  private val queue = new mutable.Queue[K]()

  def put(value: V) {
    put(identifier(value), value)
  }

  def put(id: K, value: V) {
    cache.put(id, value)
    queue.enqueue(id)
    if (queue.size > max) {
      val lost = queue.dequeue()
      cache.remove(lost)
    }
  }

  def get(id: K): Option[V] = cache.get(id)

  def apply(id: K): V = cache(id)

  def pop(): V = {
    val last = queue.dequeue()
    cache(last)
  }

  def clear() {
    cache.clear()
    queue.clear()
  }

  def size: Int = cache.size

  def isEmpty: Boolean = cache.isEmpty

}

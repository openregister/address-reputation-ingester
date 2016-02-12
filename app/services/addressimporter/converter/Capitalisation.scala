/*
 * Copyright 2016 HM Revenue & Customs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package services.addressimporter.converter

import scala.collection.immutable.HashSet


object Capitalisation {
  def normalise(phrase: String):String = {
    val words: Seq[String] = phrase.split(' ')

    if (words.length == 1) asFirstWord(words.head)
    else asFirstWord(words.head) + words.tail.map(asOtherWord).mkString(" ", " ", "")
  }


  def normaliseAddressLine(line: String): String = normalise(line.trim.toLowerCase)


  val stopWords = HashSet(
    // English stop words
    "and", "at", "by", "cum", "in", "next", "of", "on", "the", "to", "under", "upon", "with",
    // "but" isn't included because it's often a proper name too
    // French loan words
    "de", "en", "la", "le",
    // Welsh stop words
    "y", "yr",
    // Gaelic and Cornish stop words
    "an", "na", "nam"
  )

  val contractedPrefixes = Set("a'", "d'", "o'")

  val subwordSpecialCases = Map("i'anson" -> "I'Anson") // DL3 0RL


  def capitaliseRestOfSubwords(word: String): String =
    if (stopWords.contains(word)) word else capitaliseSpecialCases(word)

  def capitaliseSpecialCases(lcWord: String): String =
    subwordSpecialCases.get(lcWord).fold(capitaliseWithContractedPrefix(lcWord)) { specialCase => specialCase }


  def capitaliseWithContractedPrefix(word: String): String =
    if (word.length < 2) word.capitalize
    else {
      val two = word.take(2)
      if (contractedPrefixes.contains(two)) two.capitalize + word.drop(2).capitalize
      else word.capitalize
    }

  def conjoin(first: String, rest: Seq[String]): String =
    if (rest.isEmpty) first
    else first + rest.map(capitaliseRestOfSubwords).mkString("-", "-", "")

  def subwords(phrase: String): Seq[String] = phrase.split('-')

  def asFirstWord(word: String): String = {
    val sub = subwords(word)
    if (sub.nonEmpty) conjoin(sub.head.capitalize, sub.tail) else "-"
  }

  def asOtherWord(word: String): String = {
    val sub = subwords(word)
    if (sub.nonEmpty) conjoin(capitaliseRestOfSubwords(sub.head), sub.tail) else "-"
  }

}

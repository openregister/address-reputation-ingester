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

package ingest.algorithm

object Algorithm {
  def apply(
             include: Option[String],
             prefer: Option[String],
             streetFilter: Option[Int]
           ): Algorithm = {
    val inc = include getOrElse "DPA,LPI"
    new Algorithm(
      inc.contains("DPA"),
      inc.contains("LPI"),
      prefer.getOrElse("DPA"),
      streetFilter.getOrElse(1))
  }
}


case class Algorithm(
                      includeDpa: Boolean = true,
                      includeLpi: Boolean = true,
                      prefer: String = "DPA",
                      streetFilter: Int = 1
                    ) {

  val containedPhrases: List[String] =
    streetFilter match {
      case 1 =>
        List(
          "from ", "pump ", "pumping ", "mast ", "hydraulic ram", "helipad ", "across from", "fire station",
          "awaiting conversion", "ppg sta", "footway", "bridge", "pipeline", "redevelopment")

      case 2 =>
        List(
          " adjacent to ",
          " adj to ",
          " to east of ",
          " to the east of ",
          " to north of ",
          " to the north of ",
          " to rear of ",
          " to the rear of ",
          " to south of ",
          " to the south of ",
          " to west of ",
          " to the west of ")

      case _ => Nil
    }

  val startingPhrases: List[String] =
    streetFilter match {
      case 2 =>
        List(
          "access to ",
          "adjacent to ",
          "adj to ",
          "back lane from ",
          "back lane to ",
          "bus shelter ",
          "car park ",
          "drive from ",
          "footpath from ",
          "footpath next ",
          "footpath to ",
          "grass verge ",
          "landlords supply ",
          "landlord's supply ",
          "lane from ",
          "lane to ",
          "path leading from ",
          "path leading to ",
          "public footpath to ",
          "road from ",
          "road to ",
          "site supply to ",
          "street from ",
          "street to ",
          "supply to ",
          "track from ",
          "track to ")

      case _ => Nil
    }
}


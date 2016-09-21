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

package addressbase

import uk.co.hmrc.address.osgb.Document
import uk.co.hmrc.address.services.Capitalisation

object OSLpi {
  val RecordId = "24"

  val idx = OSLpiIdx(
    uprn = 3,
    logicalStatus = 6,
    saoStartNumber = 11,
    saoStartSuffix = 12,
    saoEndNumber = 13,
    saoEndSuffix = 14,
    saoText = 15,
    paoStartNumber = 16,
    paoStartSuffix = 17,
    paoEndNumber = 18,
    paoEndSuffix = 19,
    paoText = 20,
    usrn = 21)

  def apply(csv: Array[String]): OSLpi = OSLpi(
    csv(idx.uprn).toLong,
    csv(idx.logicalStatus).head,
    csv(idx.saoStartNumber).trim,
    csv(idx.saoStartSuffix).trim,
    csv(idx.saoEndNumber).trim,
    csv(idx.saoEndSuffix).trim,
    csv(idx.saoText).trim,
    csv(idx.paoStartNumber).trim,
    csv(idx.paoStartSuffix).trim,
    csv(idx.paoEndNumber).trim,
    csv(idx.paoEndSuffix).trim,
    csv(idx.paoText).trim,
    csv(idx.usrn).toLong)
}

case class OSLpiIdx(uprn: Int,
                    logicalStatus: Int,
                    saoStartNumber: Int,
                    saoStartSuffix: Int,
                    saoEndNumber: Int,
                    saoEndSuffix: Int,
                    saoText: Int,
                    paoStartNumber: Int,
                    paoStartSuffix: Int,
                    paoEndNumber: Int,
                    paoEndSuffix: Int,
                    paoText: Int,
                    usrn: Int)

case class OSLpi(uprn: Long,
                 logicalStatus: Char,
                 saoStartNumber: String,
                 saoStartSuffix: String,
                 saoEndNumber: String,
                 saoEndSuffix: String,
                 saoText: String,
                 paoStartNumber: String,
                 paoStartSuffix: String,
                 paoEndNumber: String,
                 paoEndSuffix: String,
                 paoText: String,
                 usrn: Long) extends Document {

  private def numRange(sNum: String, sSuf: String, eNum: String, eSuf: String) = {
    val start = (sNum + sSuf).trim
    val end = (eNum + eSuf).trim
    (start, end) match {
      case ("", "") => ""
      case (s, "") => s
      case ("", e) => e
      case (s, e) => s + "-" + e
    }
  }

  def primaryNumberRange: String = numRange(paoStartNumber, paoStartSuffix, paoEndNumber, paoEndSuffix)

  def secondaryNumberRange: String = numRange(saoStartNumber, saoStartSuffix, saoEndNumber, saoEndSuffix)

  // For use as input to MongoDbObject (hence it's not a Map)
  def tupled: List[(String, Any)] = List(
    "uprn" -> uprn,
    "logicalStatus" -> logicalStatus,
    "saoStartNumber" -> saoStartNumber,
    "saoStartSuffix" -> saoStartSuffix,
    "saoEndNumber" -> saoEndNumber,
    "saoEndSuffix" -> saoEndSuffix,
    "saoText" -> saoText,
    "paoStartNumber" -> paoStartNumber,
    "paoStartSuffix" -> paoStartSuffix,
    "paoEndNumber" -> paoEndNumber,
    "paoEndSuffix" -> paoEndSuffix,
    "paoText" -> paoText,
    "usrn" -> usrn)

  def normalise: OSLpi = new OSLpi(uprn,
    logicalStatus,
    saoStartNumber,
    saoStartSuffix,
    saoEndNumber,
    saoEndSuffix,
    Capitalisation.normaliseAddressLine(saoText),
    paoStartNumber,
    paoStartSuffix,
    paoEndNumber,
    paoEndSuffix,
    Capitalisation.normaliseAddressLine(paoText),
    usrn)
}


/*
 *
 *  *
 *  *  * Copyright 2016 HM Revenue & Customs
 *  *  *
 *  *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  *  * you may not use this file except in compliance with the License.
 *  *  * You may obtain a copy of the License at
 *  *  *
 *  *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *  *
 *  *  * Unless required by applicable law or agreed to in writing, software
 *  *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  * See the License for the specific language governing permissions and
 *  *  * limitations under the License.
 *  *
 *
 *
 */

package fetch

import java.net.URL


/**
  * WebDavFile is essentially an abstraction of Sardine's DavResource,
  * pimped as a case class. Note that we cannot rely on the file modification
  * timestamps on the remote server.
  */
case class WebDavFile(url: URL, fullName: String, kb: Long,
                      isDirectory: Boolean = false, isPlainText: Boolean = false, isDataFile: Boolean = false,
                      files: List[WebDavFile] = Nil) {

  val name = {
    val dot = fullName.lastIndexOf('.')
    if (dot < 0) fullName
    else fullName.substring(0, dot)
  }

  override def toString: String = indentedString("")

  private def indentedString(i: String): String = {
    val length = if (isDirectory) "" else " %10d KiB".format(kb)
    val slash = if (isDirectory) "/" else ""
    val zip = if (isDataFile) " (data)" else ""
    val txt = if (isPlainText) " (txt) " else ""
    s"$i$fullName$slash$txt$zip$length" +
      files.map(_.indentedString(i + "  ")).mkString("\n", "", "")
  }
}


/**
  * Wraps a tree of WebDavFiles such that the OSGBProducts can be extracted easily.
  */
case class WebDavTree(root: WebDavFile) {

  def findLatestFor(product: String): Option[OSGBProduct] = {
    val available: List[OSGBProduct] = findAvailableFor(product).sorted
    available.reverse.headOption
  }

  def findAvailableFor(product: String): List[OSGBProduct] = {
    val matches = root.files.find(_.fullName == product)
    if (matches.isEmpty) Nil
    else {
      val epochs = matches.get.files
      epochs.flatMap {
        extractOne(product, _)
      }
    }
  }

  def findAvailableFor(product: String, epoch: String): Option[OSGBProduct] = {
    val matches = root.files.find(_.fullName == product)
    if (matches.isEmpty) None
    else {
      val epochs = matches.get.files.find(_.fullName == epoch)
      if (epochs.isEmpty) None
      else extractOne(product, epochs.get)
    }
  }

  private def extractOne(product: String, epoch: WebDavFile): Option[OSGBProduct] = {
    val fullFolder = epoch.files.filter(_.fullName == "full")
    if (fullFolder.isEmpty) None
    else {
      val files = fullFolder.head.files
      val possibleDownloads: List[WebDavFile] = filterZipsWithTxt(files)
      if (possibleDownloads.isEmpty) None
      else Some(OSGBProduct(product, epoch.fullName.toInt, possibleDownloads))
    }
  }

  private def filterZipsWithTxt(files: List[WebDavFile]): List[WebDavFile] = {
    val subs = files.filter(_.isDirectory).flatMap(d => filterZipsWithTxt(d.files))
    val names = files.map(_.name).toSet
    val txtAndZipNames = names.filter {
      n =>
        files.exists(f => f.isPlainText && f.name == n) ||
          files.exists(f => f.isDataFile && f.name == n)
    }
    val allPairsExist = txtAndZipNames.forall {
      n =>
        files.exists(f => f.isPlainText && f.name == n) &&
          files.exists(f => f.isDataFile && f.name == n)
    }
    val chosen = if (allPairsExist) files.filter(_.isDataFile) else Nil
    chosen ++ subs
  }
}


case class OSGBProduct(productName: String, epoch: Int, zips: List[WebDavFile]) extends Ordered[OSGBProduct] {

  override def compare(that: OSGBProduct): Int = this.epoch - that.epoch
}


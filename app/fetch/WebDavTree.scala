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
case class WebDavFile(url: URL, fullName: String, kb: Long = 0L,
                      isDirectory: Boolean = false, isPlainText: Boolean = false, isDataFile: Boolean = false,
                      files: List[WebDavFile] = Nil) {

  val name = {
    val dot = fullName.lastIndexOf('.')
    if (dot < 0) fullName
    else fullName.substring(0, dot)
  }

  def indentedString(i: String = ""): String = {
    if (isDirectory) {
      s"$i$fullName/" +
        files.map(_.indentedString(i + "  ")).mkString("\n", "", "")
    } else {
      val length = " %10d KiB".format(kb)
      val mark = if (isDataFile) " (data)" else if (isPlainText) " (txt) " else "       "
      "%s%-50s%s %10d KiB".format(i, fullName, mark, kb) +
        files.map(_.indentedString(i + "  ")).mkString("\n", "", "")
    }
  }

  def find(name: String): Option[WebDavFile] = {
    if (fullName == name) Some(this)
    else {
      files.flatMap {
        child =>
          child.find(name)
      }.headOption
    }
  }
}


/**
  * Wraps a tree of WebDavFiles such that the OSGBProducts can be extracted easily.
  */
case class WebDavTree(root: WebDavFile) {

  import WebDavTree._

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

  def findAvailableFor(product: String, epoch: Int): Option[OSGBProduct] = {
    findAvailableFor(product).find(_.epoch == epoch)
  }

  private def extractOne(product: String, epoch: WebDavFile): Option[OSGBProduct] = {
    val optFullFolder = epoch.files.find(_.fullName == "full")
    if (optFullFolder.isEmpty) None
    else {
      val fullFolder = optFullFolder.get
      val readyToCollect = fullFolder.find(readyToCollectFile).isDefined
      if (readyToCollect) Some(OSGBProduct(product, epoch.fullName.toInt, filterZips(fullFolder.files)))
      else None
    }
  }

  private def filterZips(files: List[WebDavFile]): List[WebDavFile] = {
    val (subDirs, others) = files.partition(_.isDirectory)
    val subs = subDirs.flatMap(d => filterZips(d.files))
    val chosen = files.filter(_.isDataFile)
    chosen ++ subs
  }

  def indentedString: String = root.indentedString("")
}


object WebDavTree {
  val readyToCollectFile = "ready-to-collect.txt"
}


case class OSGBProduct(productName: String, epoch: Int, zips: List[WebDavFile]) extends Ordered[OSGBProduct] {

  override def compare(that: OSGBProduct): Int = this.epoch - that.epoch
}


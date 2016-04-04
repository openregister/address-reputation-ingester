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

package services.ingester.ftpdownloader

import java.text.SimpleDateFormat

import org.apache.commons.net.ftp.FTPConnectionClosedException
import org.scalatest.FunSuite
import org.scalatest.Matchers

import scala.util.Try

class FtpDownloaderTest extends FunSuite with Matchers {

  val simpleDateFormatter = new SimpleDateFormat("EEE MMM Dd HH:mm:ss zzz yyyy")


  trait DummyFtp extends FileIO {
    override def folder(dir: String): Try[List[String]] = Try {
      List.empty[String]
    }

    override def files(dir: String): Try[List[OsFile]] = Try {
      List.empty[OsFile]
    }

    override def retrieveFile(from: String, to: String): Try[Boolean] = Try {
      true
    }

    override def login(username: String, password: String, server: String, port: Int): Try[Boolean] = Try {
      true
    }

    override def createLocalFolder(dir: String): Try[Unit] = Try {}
  }


  test("check for empty folder") {
    val testFtp = new FtpDownloader with DummyFtp

    val result = testFtp.ftpDownload("/os-from", "localTemp")
    assert(result.get === ( (true, "Unknown") ))
  }

  test("check for empty sub folder") {

    val testFtp = new FtpDownloader with DummyFtp {
      override def folder(dir: String): Try[List[String]] = Try {
        dir match {
          case "/os-from" => List("DS1234567")
          case "/os-from/DS1234567" => List.empty[String]
          case _ => throw new FTPConnectionClosedException("Invalid folder: " + dir)
        }
      }
    }

    val result = testFtp.ftpDownload("/os-from", "localTemp")
    assert(result.get === ( (true, "Unknown") ))
  }

  test("error(eg network break) while finding sub folder") {
    val testFtp = new FtpDownloader with DummyFtp {
      override def folder(dir: String): Try[List[String]] = Try {
        dir match {
          case "/os-from" => List("DS1234567")
          case "/os-from/DS1234567" => throw new FTPConnectionClosedException("Network has broken: " + dir)
          case _ => throw new FTPConnectionClosedException("Invalid folder: " + dir)
        }
      }
    }


    val result = testFtp.ftpDownload("/os-from", "localTemp")

    result.isFailure shouldBe true
    result.failed.get shouldBe a[FTPConnectionClosedException]

    result.failed.get.getMessage shouldBe "Failure(org.apache.commons.net.ftp.FTPConnectionClosedException: Network has broken: /os-from/DS1234567)"

  }


  test("Check file on server for less that 24 hours") {
    val fileCreationTime = simpleDateFormatter.parse("Wed Feb 03 11:58:00 GMT 2016").getTime
    val timeNow = simpleDateFormatter.parse("Wed Feb 03 12:58:00 GMT 2016").getTime

    val testFtp = new FtpDownloader with DummyFtp
    val result = testFtp.withinValidTime(fileCreationTime, timeNow)
    assert(result === true)

  }

  test("Check file on server for more than 24 hours") {
    val fileCreationTime = simpleDateFormatter.parse("Wed Feb 03 11:58:00 GMT 2016").getTime
    val timeNow = simpleDateFormatter.parse("Wed Feb 04 12:58:00 GMT 2016").getTime

    val testFtp = new FtpDownloader with DummyFtp
    val result = testFtp.withinValidTime(fileCreationTime, timeNow)
    assert(result === false)

  }

  test("Return all files older than 24 hours") {
    val fileCreationTime = simpleDateFormatter.parse("Mon Feb 01 11:58:00 GMT 2016").getTime
    val timeNow = simpleDateFormatter.parse("Wed Feb 03 12:58:00 GMT 2016").getTime

    val files = List[OsFile](OsFile("file1.zip", "/from-os/DS1234567", fileCreationTime, size = 1))

    val testFtp = new FtpDownloader with DummyFtp
    val result = testFtp.oldFiles(timeNow, files)

    assert(result.size === 1)

  }

  test("Don't return files newer than 24 hours") {
    val fileCreationTime = simpleDateFormatter.parse("Wed Feb 03 11:58:00 GMT 2016").getTime
    val timeNow = simpleDateFormatter.parse("Wed Feb 03 12:58:00 GMT 2016").getTime

    val files = List[OsFile](OsFile("file1.zip", "/from-os/DS1234567", fileCreationTime, size = 1))

    val testFtp = new FtpDownloader with DummyFtp
    val result = testFtp.oldFiles(timeNow, files)

    assert(result.size === 0)

  }

  test("Return only files that are older than 24 hours not newer") {
    val fileCreationTime1 = simpleDateFormatter.parse("Fri Jan 08 11:58:00 GMT 2016").getTime //Older than 24 hours
    val fileCreationTime2 = simpleDateFormatter.parse("Wed Feb 03 11:58:00 GMT 2016").getTime //Less that 24 hours

    val timeNow = simpleDateFormatter.parse("Wed Feb 03 12:58:00 GMT 2016").getTime

    val files = List[OsFile](
      OsFile("file1.zip", "/from-os/DS1234567", fileCreationTime1, size = 1),
      OsFile("file2.zip", "/from-os/DS1234567", fileCreationTime2, size = 2)
    )

    val testFtp = new FtpDownloader with DummyFtp
    val result = testFtp.oldFiles(timeNow, files)

    assert(result.size === 1)
    assert(result.head.name === "file1.zip")

  }

  test("Download files from a given directory") {
    val testFtp = new FtpDownloader with DummyFtp {
      override def files(dir: String): Try[List[OsFile]] = Try {
        val fileCreationTime = simpleDateFormatter.parse("Wed Feb 03 11:58:00 GMT 2016").getTime
        List[OsFile](OsFile("AddressBasePremium_COU_2015-12-24_001_csv.zip", "/from-os/DS1234567", fileCreationTime, size = 1))
      }
    }

    val timeNow = simpleDateFormatter.parse("Fri Feb 05 12:58:00 GMT 2016").getTime

    val result = testFtp.downloadFiles("/os-from", "/temp", timeNow)
    assert(result.isSuccess === true)
    assert(result.get === ( (1, "2015-12-24") ))
  }


  test("Download 5 files from a given directory") {
    val testFtp = new FtpDownloader with DummyFtp {
      override def files(dir: String): Try[List[OsFile]] = Try {
        val fileCreationTime = simpleDateFormatter.parse("Wed Feb 03 11:58:00 GMT 2016").getTime
        List[OsFile](
          OsFile("AddressBasePremium_COU_2015-12-24_001_csv.zip", "/from-os/DS1234567", fileCreationTime, size = 1),
          OsFile("AddressBasePremium_COU_2015-12-24_002_csv.zip", "/from-os/DS1234567", fileCreationTime, size = 1),
          OsFile("AddressBasePremium_COU_2015-12-24_003_csv.zip", "/from-os/DS1234567", fileCreationTime, size = 1),
          OsFile("AddressBasePremium_COU_2015-12-24_004_csv.zip", "/from-os/DS1234567", fileCreationTime, size = 1),
          OsFile("AddressBasePremium_COU_2015-12-24_005_csv.zip", "/from-os/DS1234567", fileCreationTime, size = 1)
        )
      }
    }

    val timeNow = simpleDateFormatter.parse("Fri Feb 05 12:58:00 GMT 2016").getTime

    val result = testFtp.downloadFiles("/os-from", "/temp", timeNow)
    assert(result.isSuccess === true)
    assert(result.get === ( (5, "2015-12-24") ))
  }

  test("Download 4 files leaving the 'new' file") {
    val timeNow = simpleDateFormatter.parse("Fri Feb 05 12:58:00 GMT 2016").getTime

    val testFtp = new FtpDownloader with DummyFtp {
      override def files(dir: String): Try[List[OsFile]] = Try {
        val fileCreationTime = simpleDateFormatter.parse("Wed Feb 03 11:58:00 GMT 2016").getTime
        List[OsFile](
          OsFile("AddressBasePremium_COU_2015-12-24_001_csv.zip", "/from-os/DS1234567", fileCreationTime, size = 1),
          OsFile("AddressBasePremium_COU_2015-12-24_002_csv.zip", "/from-os/DS1234567", fileCreationTime, size = 1),
          OsFile("AddressBasePremium_COU_2015-12-24_003_csv.zip", "/from-os/DS1234567", fileCreationTime, size = 1),
          OsFile("AddressBasePremium_COU_2015-12-24_004_csv.zip", "/from-os/DS1234567", fileCreationTime, size = 1),
          OsFile("AddressBasePremium_COU_2015-12-24_005_csv.zip", "/from-os/DS1234567", timeNow, size = 1)
        )
      }
    }

    val result = testFtp.downloadFiles("/os-from", "/temp", timeNow)
    assert(result.isFailure === true, "-- result was a fail")
    result.failed.get shouldBe a[UnfinishedDownloadException]
    assert(result.failed.get.getMessage === "Not all were downloaded: missing 1 out of 5")
  }

  test("Download files from a given directory, with network error") {
    val testFtp = new FtpDownloader with DummyFtp {
      override def files(dir: String): Try[List[OsFile]] = Try {
        val fileCreationTime = simpleDateFormatter.parse("Wed Feb 03 11:58:00 GMT 2016").getTime
        List[OsFile](
          OsFile("AddressBasePremium_COU_2015-12-24_001_csv.zip", "/from-os/DS1234567", fileCreationTime, size = 1),
          OsFile("AddressBasePremium_COU_2015-12-24_002_csv.zip", "/from-os/DS1234567", fileCreationTime, size = 1),
          OsFile("AddressBasePremium_COU_2015-12-24_003_csv.zip", "/from-os/DS1234567", fileCreationTime, size = 1),
          OsFile("AddressBasePremium_COU_2015-12-24_004_csv.zip", "/from-os/DS1234567", fileCreationTime, size = 1),
          OsFile("AddressBasePremium_COU_2015-12-24_005_csv.zip", "/from-os/DS1234567", fileCreationTime, size = 1)
        )
      }

      var count = 0

      override def retrieveFile(from: String, to: String): Try[Boolean] = Try {
        count += 1
        if (count == 3) throw new FTPConnectionClosedException("Network break") else true
      }

    }

    val timeNow = simpleDateFormatter.parse("Fri Feb 05 12:58:00 GMT 2016").getTime

    val result = testFtp.downloadFiles("/os-from", "/temp", timeNow)
    assert(result.isFailure === true)
    assert(result.failed.get.getMessage === "Network break")
  }

  test("check filename splits on date") {
    val testFile = List(OsFile(name = "AddressBasePremium_COU_2015-12-24_005_csv.zip", "", 0L, 0L))

    val testFtp = new FtpDownloader with DummyFtp

    val result = testFtp.folderName(testFile)

    assert(result === Some("2015-12-24"))
  }

  test("check error on filename split") {
    val testFile = List(OsFile(name = "Readme.zip", "", 0L, 0L))

    val testFtp = new FtpDownloader with DummyFtp
    val result = testFtp.folderName(testFile)

    assert(result === None)
  }

  test("Check error when invalid file is downloaded from a given directory") {
    val fileCreationTime = simpleDateFormatter.parse("Wed Feb 03 11:58:00 GMT 2016").getTime
    val testFtp = new FtpDownloader with DummyFtp {
      override def files(dir: String): Try[List[OsFile]] = Try {
        List(new OsFile("Readme.zip", "/from-os/DS1234567", fileCreationTime, size = 1))
      }
    }

    val result = testFtp.downloadFiles("", "/temp", 0L)
    assert(result.isFailure === true)
    assert(result.failed.get.getMessage === "Not all were downloaded: missing 1 out of 1")
  }

  test("Download a file from a given directory") {
    val testFtp = new FtpDownloader with DummyFtp {
      override def retrieveFile(from: String, to: String): Try[Boolean] = Try {
        assert(from === "/from-os/DS1234567/AddressBasePremium_COU_2015-12-24_005_csv.zip", "'from' file missmatch")
        true
      }
    }
    val fileCreationTime = simpleDateFormatter.parse("Wed Feb 03 11:58:00 GMT 2016").getTime
    val aFile = new OsFile("AddressBasePremium_COU_2015-12-24_005_csv.zip", "/from-os/DS1234567", fileCreationTime, size = 1)

    val result = testFtp.downloadAFile(aFile, "/temp")
    assert(result.isSuccess === true)
  }


  test("No files or folders in home directory") {
    val testFtp = new FtpDownloader with DummyFtp {
      override def folder(dir: String): Try[List[String]] = Try {
        dir match {
          case "/os-from" => List.empty[String]
          case _ => throw new FTPConnectionClosedException("Invalid folder: " + dir)
        }
      }
    }

    val result = testFtp.ftpDownload("/os-from", "localTemp")
    assert(result.get === ( (true, "Unknown") ))
  }


  test("1 zip in home folder") {
    val testFtp = new FtpDownloader with DummyFtp {
      override def folder(dir: String): Try[List[String]] = Try {
        dir match {
          case "/os-from" => List.empty[String]
          case _ => throw new FTPConnectionClosedException("Invalid folder: " + dir)
        }
      }

      override def files(dir: String): Try[List[OsFile]] = Try {
        val fileCreationTime = simpleDateFormatter.parse("Wed Feb 03 11:58:00 GMT 2016").getTime
        List[OsFile](
          OsFile("AddressBasePremium_COU_2015-12-24_001_csv.zip", "/from-os/DS1234567", fileCreationTime, size = 1)
        )
      }
    }


    val result = testFtp.ftpDownload("/os-from", "localTemp")
    assert(result === Try {
      (true, "2015-12-24")
    })
  }


  test("1 zip in main with 1 empty sub folder") {
    val testFtp = new FtpDownloader with DummyFtp {
      override def folder(dir: String): Try[List[String]] = Try {
        dir match {
          case "/os-from" => List("DS1234567")
          case "/os-from/DS1234567" => List.empty[String]
          case _ => throw new FTPConnectionClosedException("Invalid folder: " + dir)
        }
      }

      override def files(dir: String): Try[List[OsFile]] = Try {
        val fileCreationTime = simpleDateFormatter.parse("Wed Feb 03 11:58:00 GMT 2016").getTime
        List[OsFile](
          OsFile("AddressBasePremium_COU_2015-12-24_001_csv.zip", "/from-os/DS1234567", fileCreationTime, size = 1)
        )
      }
    }


    val result = testFtp.ftpDownload("/os-from", "localTemp")
    assert(result === Try {
      (true, "2015-12-24")
    })
  }


  test("1 zip in main with 1 zip in sub folder") {
    val testFtp = new FtpDownloader with DummyFtp {
      override def folder(dir: String): Try[List[String]] = Try {
        dir match {
          case "/os-from" => List("DS1234567")
          case "/os-from/DS1234567" => List.empty[String]
          case _ => throw new FTPConnectionClosedException("Invalid folder: " + dir)
        }
      }

      override def files(dir: String): Try[List[OsFile]] = Try {
        dir match {
          case "/os-from" =>
            val fileCreationTime = simpleDateFormatter.parse("Wed Feb 03 11:58:00 GMT 2016").getTime
            List[OsFile](
              OsFile("AddressBasePremium_COU_2015-12-24_001_csv.zip", "/from-os/DS1234567", fileCreationTime, size = 1)
            )
          case "/os-from/DS1234567" =>
            val fileCreationTime = simpleDateFormatter.parse("Wed Feb 03 11:58:00 GMT 2016").getTime
            List[OsFile](
              OsFile("AddressBasePremium_COU_2015-12-24_002_csv.zip", "/from-os/DS1234567", fileCreationTime, size = 1)
            )
          case _ => List.empty[OsFile]

        }
      }
    }


    val result = testFtp.ftpDownload("/os-from", "localTemp")
    assert(result === Try {
      (true, "2015-12-24")
    })
  }
}

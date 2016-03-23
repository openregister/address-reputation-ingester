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

import org.scalatest.FunSuite

class CSVLineSuite extends FunSuite {

  test("check final formatting of a single csv line") {

    assert(CSVLine(1L, "", "", "", "", "").toString() === """1,"","","","",""", "Empty list")

    assert(
      CSVLine(1L, "1 UPPER KENNERTY MILL COTTAGES", "ST ANDREWS PARISH CHURCH", "WEST JESMOND", "NEWCASTLE UPON TYNE", "G77 6RT").toString()
        ===
        """1,"1 Upper Kennerty Mill Cottages","St Andrews Parish Church","West Jesmond","Newcastle upon Tyne",G77 6RT""", "full"
    )

    assert(
      CSVLine(1L, "1 UPPER KENNERTY MILL COTTAGES", "ST ANDREWS PARISH CHURCH", "", "NEWCASTLE UPON TYNE", "G77 6RT").toString()
        ===
        """1,"1 Upper Kennerty Mill Cottages","St Andrews Parish Church","","Newcastle upon Tyne",G77 6RT""", "full"
    )

    assert(
      CSVLine(1L, "1 UPPER KENNERTY MILL COTTAGES", "", "", "NEWCASTLE UPON TYNE", "").toString()
        ===
        """1,"1 Upper Kennerty Mill Cottages","","","Newcastle upon Tyne",""", "town"
    )

    assert(
      CSVLine(1L, "1 UPPER KENNERTY MILL COTTAGES", "", "", "NEWCASTLE-UPON-TYNE", "").toString()
        ===
        """1,"1 Upper Kennerty Mill Cottages","","","Newcastle-upon-Tyne",""", "town with '-'"
    )
  }
}

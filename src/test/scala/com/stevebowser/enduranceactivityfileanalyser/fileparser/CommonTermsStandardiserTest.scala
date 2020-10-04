package com.stevebowser.enduranceactivityfileanalyser.fileparser

import com.stevebowser.enduranceactivityfileanalyser.fileparser.CommonTermsStandardiser.matchActivityType
import org.scalatest.flatspec.AnyFlatSpec

class CommonTermsStandardiserTest extends AnyFlatSpec{

  "matchActivityType" should "return run when matching against running" in {
    //arrange
    val input = "running"
    //act
    val expectedValue = "run"
    val actualVale = matchActivityType(input)
    //asset
    assert(expectedValue == actualVale)
  }

  it should "return unknown for a term which is not in the dictionary of pattern matching" in {
    //arrange
    val input = "unknownActivityType"
    //act
    val expectedValue = "unknown"
    val actualVale = matchActivityType(input)
    //asset
    assert(expectedValue == actualVale)
  }


}
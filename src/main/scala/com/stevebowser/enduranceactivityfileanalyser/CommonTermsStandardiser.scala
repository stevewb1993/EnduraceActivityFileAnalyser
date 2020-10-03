package com.stevebowser.enduranceactivityfileanalyser

object CommonTermsStandardiser {

  def matchActivityType (activityName: String) : String = {
    val runPattern = "run".r
    val cyclePattern = "cycle".r
    val swimPattern = "swim".r

    if (runPattern.findFirstIn(activityName.toLowerCase()).nonEmpty) "Run"
    else if (cyclePattern.findFirstIn(activityName.toLowerCase()).nonEmpty) "Cycle"
    else if (swimPattern.findFirstIn(activityName.toLowerCase()).nonEmpty) "Swim"
    else "unknown"
  }
}

package com.stevebowser.enduranceactivityfileanalyser.fileparser

private object CommonTermsStandardiser {

  def matchActivityType (activityName: String) : String = {
    val runPattern = "run".r
    val cyclePattern = "cycle".r
    val swimPattern = "swim".r

    if (runPattern.findFirstIn(activityName.toLowerCase()).nonEmpty) "run"
    else if (cyclePattern.findFirstIn(activityName.toLowerCase()).nonEmpty) "cycle"
    else if (swimPattern.findFirstIn(activityName.toLowerCase()).nonEmpty) "swim"
    else "unknown"
  }
}

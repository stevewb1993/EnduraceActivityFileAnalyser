# EnduranceActivityFileAnalyser

This spark package can be used for analysing activity files for endurance activities (currently cycling and running). Current features:

### File parser
Converts activity files to a standardised format suitable for analysis. Supported file types are
  - .gpx

### Analysis
 Uses the standardised format defined through the file parser to provide aggregated analytics. Features are:
  - calculatePersonalBests: provides personal bests over any given distance for each type of activity

### Future features will include:
File Parser: Support for .fit files
Analysis: generic analysis methods for all types of sensor data (power, cadence, heart rate etc)

### To access all your historic activity data: 
  - https://support.strava.com/hc/en-us/articles/216918437-Exporting-your-Data-and-Bulk-Export
  - https://forums.garmin.com/apps-software/mobile-apps-web/f/garmin-connect-web/166824/is-there-a-way-to-export-bulk-data-to-tcx-or-gpx-files-seems-like-i-can-only-bulk-export-to-csv-and-individual-activities-to-gpx

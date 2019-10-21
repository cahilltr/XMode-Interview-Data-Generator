# Generate Parquet
This module is for generate parquet that contains points within and outside of the radius of the points of interest or designated time frame.
The base data set parquet schema is as follows: advertiser_id: String, location_at: Timestamp (UTC), latitude: Double, longitude: Double.

### Arguments
| Argument        | Description           |
| ------------- |:-------------:|
| startDateMillis      | Start date to filter on|
| endDateMillis      | End date to filter on      |
| numLocations | Number of locations to generate      |
| percentLocationsNear | Percent of locations that should be "near" a point of interest      |
| outputFile | Output Parquet file location      |
| numOutOfDateLocations | Number of locations to generate that are outside of the date filter      |

#### Data
World population was found here: https://github.com/CODAIT/redrock/blob/master/twitter-decahose/src/main/resources/Location/worldcitiespop.txt.gz
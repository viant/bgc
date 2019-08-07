## August 7 2019 0.7.2
    * Update logic for retrying jobs with shared job reference  
    
## August 5 2019 0.7.1
    * Added pageSize config parameter to control MaxResult on query interator

## April 16 2019 0.7.0
    * Integrated rate limiter
    * Minor refactoring

## April 15 2019 0.6.0
    * Expose the following configuration parameters:
        - insertIdColumn
        - streamBatchCount
        - insertWaitTimeoutInMs
        - insertMaxRetires

## Jan 29 2019 0.5.3
    * Patched streaming wait for test data

## Jan 29 2019 0.5.2
    * Fix QueryIterator rowsIndex

## Jan 29 2019 0.5.0
    * Added default http clinet (to run within GCE, or with GOOGLE_APPLICATION_CREDENTIALS)

## Jan 23 2019 0.4.1
    * Updated Create Table API signature change
    * Patched show create table with cluster option

## Dec 28 2018 0.3.0
    * Patched timestamp type bind parameters substitution
    * Paralelize streaming insert 
    * Introduced streamBatchCount to control streaming batch
    * Updated detection of data avaiable in streaming buffer

## Nov 18 2018 0.2.1
    * Extended dialect with ShowCreateTable
    * Patched/enhanced conversion from TableTow to free type (i.e map[string]interface{})
    * Patched timestamp sec level conversion entrophy

## Jul 1 2016 (Alpha)

  * Initial Release.

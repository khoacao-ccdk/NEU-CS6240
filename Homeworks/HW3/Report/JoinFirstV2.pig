REGISTER s3://cs6240-hw1-cody/piggybank.jar
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader;

-- Set number of reducer tasks to 10
SET default_parallel 10;

-- Load the input from parameter
Flight = LOAD '$input' using CSVLoader();

-- Parse only required columns as Flight1
Flight1 = FOREACH Flight GENERATE $5 as FlightDate, $11 as Origin, $17 as Dest,
          $24 as DepTime, $35 as ArrTime, $37 as ArrDelayMinutes, $41 as Cancelled,
	  $43 as Diverted;
-- Parse only required columns as Flight2
Flight2 = FOREACH Flight GENERATE $5 as FlightDate, $11 as Origin, $17 as Dest,
          $24 as DepTime, $35 as ArrTime, $37 as ArrDelayMinutes, $41 as Cancelled,
	  $43 as Diverted;

-- Filter unnecessary records
Flight1_data = FILTER Flight1 BY (Origin eq 'ORD' AND Dest neq 'JFK') AND (Cancelled neq '1' OR Diverted neq '1');
Flight2_data = FILTER Flight2 BY (Origin neq 'ORD' AND Dest eq 'JFK') AND (Cancelled neq '1' OR Diverted neq '1');

-- Join Flight1_data with Flight2_data based on date and intermidiate airport
Results = JOIN Flight1_data by (FlightDate, Dest), Flight2_data by (FlightDate, Origin);

-- Filter data if the time & date range doesn't match
Results = FILTER Results BY $4 < $11;
Results = FILTER Results BY (ToDate($0,'yyyy-MM-dd') < ToDate('2008-06-01','yyyy-MM-dd')) AND
	  (ToDate($0,'yyyy-MM-dd') > ToDate('2007-05-31','yyyy-MM-dd'));

-- Calculate TotalDelay for every filterred twolegged flights
Results = FOREACH Results GENERATE (float)($5 + $13) as TotalDelay;

-- Calculating total delay and average
Grouped = GROUP Results all;
Average = FOREACH Grouped GENERATE AVG(Results.TotalDelay);

-- Storing the output to S3
STORE Average into '$output' USING PigStorage();
REGISTER file:/home/hadoop/pig/contrib/piggybank/java/piggybank.jar
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader;

-- Setting number of reducer tasks to 10
SET default_parallel 10;

-- Loads data from parameter
Flight = LOAD '$input' using CSVLoader();

-- Parse required columns as Flight1_data
Flight1_data = FOREACH Flight GENERATE $5 as FlightDate, $11 as Origin, $17 as Dest,
               $24 as DepTime, $35 as ArrTime, $37 as ArrDelayMinutes, $41 as Cancelled,
	       $43 as Diverted;

-- Parse required columns as Flight2_data
Flight2_data = FOREACH Flight GENERATE $5 as FlightDate, $11 as Origin, $17 as Dest,
	       $24 as DepTime, $35 as ArrTime, $37 as ArrDelayMinutes, $41 as Cancelled,
	       $43 as Diverted;

-- Filter unnecessary records for Flight1_data & Flight2_data
Flight1_data = FILTER Flight1_data BY (Origin eq 'ORD' AND Dest neq 'JFK') AND (Cancelled neq '1' OR Diverted neq '1')
	       AND (ToDate(FlightDate,'yyyy-MM-dd') < ToDate('2008-06-01','yyyy-MM-dd')) AND
	       (ToDate(FlightDate,'yyyy-MM-dd') > ToDate('2007-05-31','yyyy-MM-dd'));

Flight2_data = FILTER Flight2_data BY (Origin neq 'ORD' AND Dest eq 'JFK') AND (Cancelled neq '1' OR Diverted neq '1')
	       AND (ToDate(FlightDate,'yyyy-MM-dd') < ToDate('2008-06-01','yyyy-MM-dd')) AND
	       (ToDate(FlightDate,'yyyy-MM-dd') > ToDate('2007-05-31','yyyy-MM-dd'));

-- Join Flight1_data with Flight2_data based on date and transit airport
Results = JOIN Flight1_data by (FlightDate, Dest), Flight2_data by (FlightDate, Origin);
Results = FILTER Results BY $4 < $11;
Results = FILTER Results BY (ToDate($0,'yyyy-MM-dd') < ToDate('2008-06-01','yyyy-MM-dd')) AND
	  (ToDate($0,'yyyy-MM-dd') > ToDate('2007-05-31','yyyy-MM-dd'));

-- Calculate TotalDelay for every filtered flights
Results = FOREACH Results GENERATE (float)($5 + $13) as TotalDelay;

-- Calculate total delay and average
Grouped = GROUP Results all;
Average = FOREACH Grouped GENERATE AVG(Results.TotalDelay);

-- Store the output to S3
-- STORE average into '$output' USING PigStorage();

STORE Average into 'output';
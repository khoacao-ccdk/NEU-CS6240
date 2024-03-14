package neu.cs6240.Utils;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class Configuration {
  public static final Set<Character> VALID_START_WORDS =
      new HashSet<>(Arrays.asList('m','n','o','p','q','M','N','O','P','Q'));
  public static final int NUM_REDUCER = 10;
  public static final String ORIGIN_AIRPORT = "ORD";
  public static final String DEST_AIRPORT = "JFK";
  public static final int INTERVAL_START_YEAR = 2007;
  public static final int INTERVAL_START_MONTH = 6;
  public static final int INTERVAL_END_YEAR = 2008;
  public static final int INTERVAL_END_MONTH = 5;
  public static final String EXPECTED_CANCELLED_STATUS = "0.00";
  public static final String EXPECTED_DIVERTED_STATUS = "0.00";
}

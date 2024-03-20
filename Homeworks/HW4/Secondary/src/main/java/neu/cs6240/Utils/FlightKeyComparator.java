package neu.cs6240.Utils;

import org.apache.hadoop.io.WritableComparator;

/**
 * This class acts as a more extensive comparator where it compares the airlines,
 * then the month in ascending order using the FlightKey class's compareTo method.
 */
public class FlightKeyComparator extends WritableComparator {

  @Override
  public int compare(Object a, Object b) {
    FlightKey keyOne = (FlightKey) a,
              keyTwo = (FlightKey) b;

    return keyOne.compareTo(keyTwo);
  }
}

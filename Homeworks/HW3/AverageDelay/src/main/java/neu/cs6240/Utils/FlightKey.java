package neu.cs6240.Utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

/**
 * This class acts as a key tuple for the Map output
 *
 * The key stores two main information: flightDate and which leg the flight is
 */
public class FlightKey implements WritableComparable {
  public static final String LEG_ONE = "Leg1";
  public static final String LEG_TWO = "Leg2";

  private String flightDate;
  private String leg;

  public FlightKey(String flightDate, String leg) {
    this.flightDate = flightDate;
    this.leg = leg;
  }

  /**
   * Compares with another key based on the flight date
   * @param o the object to be compared.
   * @return
   */
  @Override
  public int compareTo(Object o) {
    FlightKey other = (FlightKey) o;
    return this.flightDate.compareTo(other.flightDate);
  }

  /**
   * Writes the data of the object to the output stream
   * @param out
   * @throws IOException
   */
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeBytes(flightDate);
    out.writeBytes(leg);
  }

  /**
   * Reads the data from the input stream and construcst the object
   * @param in
   * @throws IOException
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    this.flightDate = String.valueOf(in.readByte());
    this.leg = String.valueOf(in.readByte());
  }

  public String getFlightDate() {
    return flightDate;
  }

  public String getLeg() {
    return leg;
  }

  @Override
  public String toString() {
    final StringBuffer sb = new StringBuffer("FlightKey{");
    sb.append("flightDate='").append(flightDate).append('\'');
    sb.append(", leg='").append(leg).append('\'');
    sb.append('}');
    return sb.toString();
  }
}

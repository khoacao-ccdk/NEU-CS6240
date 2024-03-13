package neu.cs6240.Utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 * This class is used as the value for the output of the map phase
 */
public class FlightValue implements Writable {
  private String origin;
  private String dest;
  private String deptTime;
  private String arrTime;
  private int arrDelayMinute;

  /**
   *
   * @param origin
   * @param dest
   * @param deptTime
   * @param arrTime
   * @param arrDelayMinute
   */
  public FlightValue(String origin, String dest, String deptTime, String arrTime,
      int arrDelayMinute) {
    this.origin = origin;
    this.dest = dest;
    this.deptTime = deptTime;
    this.arrTime = arrTime;
    this.arrDelayMinute = arrDelayMinute;
  }

  /**
   * Writes the object data to the output stream
   * @param out
   * @throws IOException
   */
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeBytes(origin);
    out.writeBytes(dest);
    out.writeBytes(deptTime);
    out.writeBytes(arrTime);
    out.write(arrDelayMinute);
  }

  /**
   * Reads data from the input stream and construct the object
   * @param in
   * @throws IOException
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    this.origin = in.readLine();
    this.dest = in.readLine();
    this.deptTime = in.readLine();
    this.arrTime = in.readLine();
    this.arrDelayMinute = in.readInt();
  }

  public String getOrigin() {
    return origin;
  }

  public String getDest() {
    return dest;
  }

  public String getDeptTime() {
    return deptTime;
  }

  public String getArrTime() {
    return arrTime;
  }

  public int getArrDelayMinute() {
    return arrDelayMinute;
  }

  @Override
  public String toString() {
    final StringBuffer sb = new StringBuffer("FlightValue{");
    sb.append("origin='").append(origin).append('\'');
    sb.append(", dest='").append(dest).append('\'');
    sb.append(", deptTime='").append(deptTime).append('\'');
    sb.append(", arrTime='").append(arrTime).append('\'');
    sb.append(", arrDelayMinute=").append(arrDelayMinute);
    sb.append('}');
    return sb.toString();
  }
}

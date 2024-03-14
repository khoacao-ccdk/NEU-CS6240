package neu.cs6240.Utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * This class is used as the value for the output of the map phase
 */
public class FlightValue implements Writable {
  private Text origin;
  private Text dest;
  private Text deptTime;
  private Text arrTime;
  private IntWritable arrDelayMinute;

  public FlightValue() {
  }

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
    this.origin = new Text(origin);
    this.dest = new Text(dest);
    this.deptTime = new Text(deptTime);
    this.arrTime = new Text(arrTime);
    this.arrDelayMinute = new IntWritable(arrDelayMinute);
  }

  /**
   * Writes the object data to the output stream
   * @param out
   * @throws IOException
   */
  @Override
  public void write(DataOutput out) throws IOException {
    origin.write(out);
    dest.write(out);
    deptTime.write(out);
    arrTime.write(out);
    arrDelayMinute.write(out);
  }

  /**
   * Reads data from the input stream and construct the object
   * @param in
   * @throws IOException
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    this.origin = new Text();
    this.dest = new Text();
    this.deptTime = new Text();
    this.arrTime = new Text();
    this.arrDelayMinute = new IntWritable();

    origin.readFields(in);
    dest.readFields(in);
    deptTime.readFields(in);
    arrTime.readFields(in);
    arrDelayMinute.readFields(in);
  }

  /**
   * Performs a deep clone on the current object
   * @return a new Object with the exact same data
   */
  public FlightValue clone(){
      return new FlightValue(
            this.origin.toString(),
            this.dest.toString(),
            this.deptTime.toString(),
            this.arrTime.toString(),
            this.arrDelayMinute.get()
      );
  }

  public String getOrigin() {
    return origin.toString();
  }

  public String getDest() {
    return dest.toString();
  }

  public String getDeptTime() {
    return deptTime.toString();
  }

  public String getArrTime() {
    return arrTime.toString();
  }

  public int getArrDelayMinute() {
    return arrDelayMinute.get();
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

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    FlightValue that = (FlightValue) o;
    return Objects.equals(origin, that.origin) && Objects.equals(dest, that.dest) && Objects.equals(deptTime, that.deptTime) && Objects.equals(arrTime, that.arrTime) && Objects.equals(arrDelayMinute, that.arrDelayMinute);
  }

  @Override
  public int hashCode() {
    return Objects.hash(origin, dest, deptTime, arrTime, arrDelayMinute);
  }
}

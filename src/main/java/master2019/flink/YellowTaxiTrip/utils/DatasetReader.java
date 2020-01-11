package master2019.flink.YellowTaxiTrip.utils;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple18;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Calendar;
import java.util.Date;

/**
 * Utils to handle with Yellow Taxi Trip Dataset.
 */
public class DatasetReader {
    /**
     * Parse date to milliseconds using the Calendar to avoid parser errors.
     *
     * @param dateString Date to be parseed
     * @return Time in Milliseconds
     */
    public static Date dateStringToDate(final String dateString) {
        final String[] initTimeArray = parseDateString(dateString);
        final int year = Integer.parseInt(initTimeArray[0]);
        final int month = Integer.parseInt(initTimeArray[1]);
        final int day = Integer.parseInt(initTimeArray[2]);
        final int hour = Integer.parseInt(initTimeArray[3]);
        final int minute = Integer.parseInt(initTimeArray[4]);
        final int second = Integer.parseInt(initTimeArray[5]);

        final Calendar target = Calendar.getInstance();
        target.set(year, month - 1, day, hour, minute);
        target.set(Calendar.SECOND, second);

        return target.getTime();
    }

    /**
     * Separate date by fields that compose it: year, month, day, hour, minute and second
     *
     * @param dateString Date to be parsed
     * @return Date separated by fields: year, month, day, hour, minute and second
     */
    public static String[] parseDateString(final String dateString) {
        final String[] date = dateString.split(" ")[0].split("-");
        final String[] time = dateString.split(" ")[1].split(":");

        return new String[]{date[0], date[1], date[2], time[0], time[1], time[2]};
    }

    /**
     * Main method to read the full data from the Yellow Taxi Trip Dataset.
     *
     * @param env      flink environment
     * @param filePath File where the dataset is
     * @return Tuple with 18 values of the dataset
     */
    public static DataSet<Tuple18<Integer, String, String, Integer, Float, Integer, String, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float>>
    readFullDataset(final ExecutionEnvironment env, final String filePath) {
        return env.readCsvFile(filePath).
                types(Integer.class, String.class, String.class, Integer.class, Float.class, Integer.class, String.class,
                        Float.class, Float.class, Float.class, Float.class, Float.class, Float.class, Float.class, Float.class,
                        Float.class, Float.class, Float.class);
    }

    /**
     * Returns the necessary data for the jfk alarms exercise.
     *
     * @param env      flink environment
     * @param filePath File where the dataset is
     * @return Tuple with shape 0 => VendorID, 1 => tpep_pickup_datetime, 2 => tpep_dropoff_datetime, 3 => passenger_count, 4 => RatecodeID
     */
//    public static DataSet<Tuple5<Integer, String, String, Integer, Integer>> getJFKAlarmsParameters(final ExecutionEnvironment env, final String filePath) {
//        return readFullDataset(env, filePath).project(0, 1, 2, 3, 5);
//    }

    /**
     * Returns the necessary data for the jfk alarms exercise.
     *
     * @param env      flink environment
     * @param filePath File where the dataset is
     * @return Tuple with shape 0 => VendorID, 1 => tpep_pickup_datetime, 2 => tpep_dropoff_datetime, 3 => passenger_count, 4 => RatecodeID
     */
    public static DataStream<Tuple5<Integer, String, String, Integer, Integer>> getJFKAlarmsParameters(final StreamExecutionEnvironment env, final String filePath) {
        return env
                .readTextFile(filePath)
                .flatMap((String input, Collector<Tuple5<Integer, String, String, Integer, Integer>> collector) -> {
                    String[] row = input.split(",");
                    collector.collect(new Tuple5<>(Integer.parseInt(row[0]), row[1], row[2], Integer.parseInt(row[3]), Integer.parseInt(row[5])));
                })
                .returns(Types.TUPLE(Types.INT, Types.STRING, Types.STRING, Types.INT, Types.INT));
    }

    /**
     * Returns the necessary data for the large trips exercise.
     *
     * @param env      flink environment
     * @param filePath File where the dataset is
     * @return Tuple with shape TODO
     */
    public static DataStream<Tuple3<Integer, String, String>> getLargeTripsParameters(final StreamExecutionEnvironment env, final String filePath) {
        return env
                .readTextFile(filePath)
                .flatMap((String input, Collector<Tuple3<Integer, String, String>> collector) -> {
                    String[] row = input.split(",");
                    collector.collect(new Tuple3<>(Integer.parseInt(row[0]), row[1], row[2]));
                })
                .returns(Types.TUPLE(Types.INT, Types.STRING, Types.STRING));
    }


}

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

import java.text.SimpleDateFormat;

/**
 * Utils to handle with Yellow Taxi Trip Dataset.
 */
public class DatasetReader {
    /**
     * Simple date formatter for dataset date types.
     */
    public static final SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-mm-dd HH:mm:ss");

    public static long getTimeInSeconds(final String date) throws Exception {
        final String[] dateSplitted = date.trim().split("[ :-]");
        if (dateSplitted.length == 6) {
            final int hour = Integer.parseInt(dateSplitted[3]);
            final int minutes = Integer.parseInt(dateSplitted[4]);
            final int seconds = Integer.parseInt(dateSplitted[5]);
            return hour * 3600 + minutes * 60 + seconds;
        } else {
            throw new Exception("Error in date. Expected format yyyy-mm-dd HH:mm:ss");
        }
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
    public static DataSet<Tuple5<Integer, String, String, Integer, Integer>> getJFKAlarmsParameters(final ExecutionEnvironment env, final String filePath) {
        return readFullDataset(env, filePath).project(0, 1, 2, 3, 5);
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

package master2019.flink.YellowTaxiTrip.utils;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple18;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;

import java.text.SimpleDateFormat;
import java.util.Locale;

/**
 * Utils to handle with Yellow Taxi Trip Dataset.
 */
public class DatasetReader {
    /**
     * Simple date formatter for dataset date types.
     */
    public static final SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-mm-dd HH:mm:ss", Locale.ENGLISH);

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
    public static DataSet<Tuple3<Integer, String, String>> getLargeTripsParameters(final ExecutionEnvironment env, final String filePath) {
        return readFullDataset(env, filePath).project(0, 1, 2);
    }
}

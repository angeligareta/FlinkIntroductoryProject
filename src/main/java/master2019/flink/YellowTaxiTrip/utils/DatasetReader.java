package master2019.flink.YellowTaxiTrip.utils;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple18;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;

import java.text.SimpleDateFormat;
import java.util.Locale;

// Dataset row examples
// 1, 2019-06-01 00:29:12, 2019-06-01 01:03:13, 1, 8.60, 1, N, 186, 243, 1, 31.5, 3, 0.5, 7.05, 0, 0.3, 42.35, 2.5
// 2, 2019-06-01 00:01:48, 2019-06-01 00:16:06, 1, 1.74, 1, N, 107, 148, 1, 11, 0.5, 0.5, 2.96, 0, 0.3, 17.76, 2.5

// Dataset headers
// VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID,
//store_and_fwd_flag, PULocationID, DOLocationID, payment_type, fare_amount, extra, mta_tax,
//tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge

// Datatset types
// Int, Date, Date, Int, Float, Int, String, Int, Int, Int, Float, Float, Float, Float, Int, Float, Float, Float

public class DatasetReader {
    public static final SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-mm-dd HH:mm:ss", Locale.ENGLISH);

    /**
     * Main method to read the full data from the yellowtrip
     *
     * @param env
     * @param filePath
     * @return
     */
    public static DataSet<Tuple18<Integer, String, String, Integer, Float, Integer, String, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float>>
    readFullDataset(final ExecutionEnvironment env, final String filePath) {
        return env.readCsvFile(filePath).
                types(Integer.class, String.class, String.class, Integer.class, Float.class, Integer.class, String.class,
                        Float.class, Float.class, Float.class, Float.class, Float.class, Float.class, Float.class, Float.class,
                        Float.class, Float.class, Float.class);
    }

    public static DataSet<Tuple5<Integer, String, String, Integer, Integer>> getJFKAlarmsParameters(final ExecutionEnvironment env, final String filePath) {
        return readFullDataset(env, filePath).project(0, 1, 2, 3, 5);
    }

    public static DataSet<Tuple3<Integer, String, String>> getLargeTripsParameters(final ExecutionEnvironment env, final String filePath) {
        return readFullDataset(env, filePath).project(0, 1, 2);
    }
}

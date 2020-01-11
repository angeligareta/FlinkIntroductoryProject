package master2019.flink.YellowTaxiTrip;

import master2019.flink.YellowTaxiTrip.utils.DatasetReader;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * In this class the JFK airport trips program has to be implemented.
 * <p>
 * Goal: Inform about the trips ending at JFK airport with two or more passengers
 * each hour for each vendorID.
 * <p>
 * Output format: vendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count
 * JFK can be found in RatecodeID = 2
 */
public class JFKAlarms {

    private final static String FILE_NAME = "jfkAlarms.csv";

    public static void main(final String[] args) {
        // Get params from command line
        final ParameterTool params = ParameterTool.fromArgs(args);
        final String inputPath = params.get("input");
        final String outputPath = params.get("output");

        if (inputPath == null || outputPath == null) {
            System.err.print("Could not read input and output path from arguments.\n Check you are passing them as arguments with --input and --output");
            return;
        }

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // ID, initDate, endDate, passengerCount, RateCodeID
        final DataStream<Tuple5<Integer, String, String, Integer, Integer>> jfkAlarmsSubset = DatasetReader.getJFKAlarmsParameters(env, inputPath);

        // Get the trips in the expected output: (vendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count)
        final DataStream<Tuple4<Integer, String, String, Integer>> selected_trips_per_vendorID = jfkAlarmsSubset
                // First filter trips where destination is JFK and trips where there are two or more passengers
                .filter(row -> (row.f4 == 2) && (row.f3 >= 2))
                // Assign trip start time as timestamp
                .assignTimestampsAndWatermarks(
                        new AscendingTimestampExtractor<Tuple5<Integer, String, String, Integer, Integer>>() {
                            @Override
                            public long extractAscendingTimestamp(final Tuple5<Integer, String, String, Integer, Integer> row) {
                                return DatasetReader.dateStringToDate(row.f1).getTime();
                            }
                        }
                )
                .keyBy(0)
                // Time window of 1 hour
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                // Get sum of passenger count, take pickup time from first element and dropoff time from last element
                .apply(new WindowFunction<Tuple5<Integer, String, String, Integer, Integer>, Tuple4<Integer, String, String, Integer>, Tuple, TimeWindow>() {
                    @Override
                    public void apply(final Tuple key, final TimeWindow window, final Iterable<Tuple5<Integer, String, String, Integer, Integer>> input,
                                      final Collector<Tuple4<Integer, String, String, Integer>> out) {
                        final Iterator<Tuple5<Integer, String, String, Integer, Integer>> iterator = input.iterator();
                        String firstPickupDate = null, lastDropoffDate = null;
                        int vendorID = 0, totalPassengerCount = 0;

                        // Boolean for detecting if the element is the first item to save vendorId and pickupdate
                        boolean firstItem = true;
                        while (iterator.hasNext()) {
                            final Tuple5<Integer, String, String, Integer, Integer> next = iterator.next();

                            // If first element save vendorId and pickupdate
                            if (firstItem) {
                                vendorID = next.f0;
                                firstPickupDate = next.f1;
                                firstItem = false;
                            }

                            // If last element save dropoff date
                            if (!iterator.hasNext()) {
                                lastDropoffDate = next.f2;
                            }

                            totalPassengerCount += next.f3;
                        }

                        out.collect(new Tuple4<>(vendorID, firstPickupDate, lastDropoffDate, totalPassengerCount));
                    }
                });

        // Write to output file
        selected_trips_per_vendorID.writeAsCsv(outputPath + "/" + FILE_NAME, FileSystem.WriteMode.OVERWRITE);

        // If verbose mode is activated execute is not necessary
        final Boolean verboseMode = false;

        // Show first 20 results sorted by vendorIDby console
        if (verboseMode) {
            try {
                selected_trips_per_vendorID.print();
            } catch (final Exception e) {
                e.printStackTrace();
            }
        } else {
            try {
                env.execute();
            } catch (final Exception e) {
                e.printStackTrace();
            }
        }
    }
}

package master2019.flink.YellowTaxiTrip;

import master2019.flink.YellowTaxiTrip.utils.DatasetReader;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
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
import java.util.NoSuchElementException;

/**
 * In this class the Large trips program has to be implemented.
 * <p>
 * Goal: Reports the vendors that do 5 or more trips during 3 hours that take at least 20
 * minutes.
 * <p>
 * Expected output: VendorID, day, numberOfTrips, tpep_pickup_datetime, tpep_dropoff_datetime
 */
public class LargeTrips {

    private final static String FILE_NAME = "largeTrips.csv";
    private final static int MIN_TRIP_TIME = 20 * 60 * 1000; // 20 minutes

    public static void main(final String[] args) throws NoSuchElementException {
        // Get params from command line
        final ParameterTool params = ParameterTool.fromArgs(args);
        final String inputPath = params.get("input");
        final String outputPath = params.get("output");

        if (inputPath == null || outputPath == null) {
            System.err.print("Could not read input and output path from arguments.\n Check you are passing them as arguments with --input and --output");
            return;
        }

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // ID, initDate, endDate
        final DataStream<Tuple3<Integer, String, String>> largeTripsDataset = DatasetReader.getLargeTripsParameters(env, inputPath);

        final DataStream<Tuple5<Integer, String, Integer, String, String>> selected_trips_per_vendorID = largeTripsDataset
                // First filter by Trips > 20 minutes
                .filter(row -> {
                    long initTime = DatasetReader.dateStringToDate(row.f1).getTime();
                    long endTime = DatasetReader.dateStringToDate(row.f2).getTime();
                    return (endTime - initTime) >= MIN_TRIP_TIME;
                })
                // Assign trip start time as timestamp
                .assignTimestampsAndWatermarks(
                        new AscendingTimestampExtractor<Tuple3<Integer, String, String>>() {
                            @Override
                            public long extractAscendingTimestamp(final Tuple3<Integer, String, String> row) {
                                return DatasetReader.dateStringToDate(row.f1).getTime();
                            }
                        }
                )
                // Key By vendor Id
                .keyBy(0)
                // Time window of 3 hours
                .window(TumblingEventTimeWindows.of(Time.hours(3)))
                // Sum number of trips in the window of three hours, get first pickup date and last dropoff date and save the day
                .apply(new WindowFunction<Tuple3<Integer, String, String>, Tuple5<Integer, String, Integer, String, String>, Tuple, TimeWindow>() {
                    @Override
                    public void apply(final Tuple key, final TimeWindow window, final Iterable<Tuple3<Integer, String, String>> input,
                                      final Collector<Tuple5<Integer, String, Integer, String, String>> out) {
                        final Iterator<Tuple3<Integer, String, String>> iterator = input.iterator();
                        String firstPickupDate = "", lastDropoffDate = "";
                        Integer vendorID = 0, numberOfTrips = 0;

                        // Boolean for detecting if the element is the first item to save id and initDate
                        boolean firstItem = true;
                        while (iterator.hasNext()) {
                            final Tuple3<Integer, String, String> next = iterator.next();

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

                            numberOfTrips++;
                        }

                        final String day = firstPickupDate.split(" ")[0];
                        out.collect(new Tuple5<Integer, String, Integer, String, String>(vendorID, day, numberOfTrips, firstPickupDate, lastDropoffDate));
                    }
                })
                // Filter vendorId with 5 trips or more
                .filter(row -> row.f2 >= 5);

        // Write to output file
        selected_trips_per_vendorID
                .writeAsCsv(outputPath + "/" + FILE_NAME, FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);

        // If verbose mode is activated execute is not necessary
        final boolean verboseMode = true;
        // Show first 20 results sorted by vendorIDby console
        if (verboseMode) {
            try {
                env.execute();
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

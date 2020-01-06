package master2019.flink.YellowTaxiTrip;

import master2019.flink.YellowTaxiTrip.utils.DatasetReader;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.Date;

// Dataset variables
// VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID,
//store_and_fwd_flag, PULocationID, DOLocationID, payment_type, fare_amount, extra, mta_tax,
//tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge

// Required for this exercise
// VendorID, tpep_pickup_datetime, tpep_dropoff_datetime

// Required for output
// VendorID, day, numberOfTrips, tpep_pickup_datetime, tpep_dropoff_datetime

/**
 * In this class the Large trips program has to be implemented.
 * <p>
 * Goal: Reports the vendors that do 5 or more trips during 3 hours that take at least 20
 * minutes.
 * <p>
 * Expected output: VendorID, day, numberOfTrips, tpep_pickup_datetime, tpep_dropoff_datetime
 */
public class LargeTrips {
    public static void main(final String[] args) throws Exception {
        // Get params from command line
        final ParameterTool params = ParameterTool.fromArgs(args);
        final String inputPath = params.get("input");
        final String outputPath = params.get("output");

        if (inputPath == null || outputPath == null) {
            System.err.print("Could not read input and output path from arguments.\n Check you are passing them as arguments with --input and --output");
            return;
        }

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final DataStream<Tuple3<Integer, String, String>> largeTripsDataset = DatasetReader.getLargeTripsParameters(env, inputPath);

        final DataStream<Tuple4<Integer, Integer, String, String>> selected_trips_per_vendorID =
                largeTripsDataset
                        // First filter trips that take at least 20 minutes
                        .filter((Tuple3<Integer, String, String> row) -> {
                            try {
                                final Date pickupDate = DatasetReader.dateFormatter.parse(row.f1);
                                final Date dropoffDate = DatasetReader.dateFormatter.parse(row.f2);
                                final long differenceInMilliseconds = dropoffDate.getTime() - pickupDate.getTime();
                                final int differenceInMinutes = (int) ((differenceInMilliseconds / 1000) / 60);
                                return (differenceInMinutes >= 20);
                            } catch (Exception e) {
                                // DO THIS happen in your PC? Try puting sysout
                                //System.out.println("Error");
                                return false; // This only happen in windows
                            }
                        })
                        // Add new variables, yearAndMonth and day to group by
                        // Output => VendorID, yearAndMonth, day, number_trips, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count
                        .flatMap((Tuple3<Integer, String, String> row, Collector<Tuple6<Integer, String, String, Integer, String, String>> out) -> {
                            final String[] dateSplitted = row.f1.trim().split("[ :-]");
                            if (dateSplitted.length == 6) {
                                final String yearAndMonth = dateSplitted[0] + "-" + dateSplitted[1];
                                final String day = dateSplitted[2];
                                out.collect(new Tuple6<>(row.f0, yearAndMonth, day, 1, row.f1, row.f2));
                            }
                        })
                        .returns(Types.TUPLE(Types.INT, Types.STRING, Types.STRING, Types.INT, Types.STRING, Types.STRING))
                        // Assign started time in seconds as timestamp
                        .assignTimestampsAndWatermarks(
                                new AscendingTimestampExtractor<Tuple6<Integer, String, String, Integer, String, String>>() {
                                    @Override
                                    public long extractAscendingTimestamp(final Tuple6<Integer, String, String, Integer, String, String> row) {
                                        try {
                                            final long startedTimeInSeconds = DatasetReader.getTimeInSeconds(row.f4);
                                            return startedTimeInSeconds * 1000;
                                        } catch (final Exception e) {
                                            e.printStackTrace();
                                            return 0;
                                        }
                                    }
                                }
                        )
                        // Group by vendorId, year-month and day
                        .keyBy(0, 1, 2)
                        // Make time window of 3 hours in seconds
                        .timeWindow(Time.hours(3))
                        // Add number of trips per window of three hours and take min of pick time and max of dropoff
                        .sum(3)
                        // Filter vendorId with more than 5 trips per window
                        .filter(row -> row.f3 >= 5)
                        // Project only asked columns
                        .project(0, 3, 4, 5);


        // Write to output file
        selected_trips_per_vendorID.writeAsCsv(outputPath, FileSystem.WriteMode.OVERWRITE);

        // If verbose mode is activated execute is not necessary
        final Boolean verboseMode = true;

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

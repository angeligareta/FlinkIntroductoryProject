package master2019.flink.YellowTaxiTrip;

import master2019.flink.YellowTaxiTrip.utils.DatasetReader;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

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
    public static void main(final String[] args) {
        // Get params from command line
        final ParameterTool params = ParameterTool.fromArgs(args);
        final String inputPath = params.get("input");
        final String outputPath = params.get("output");

        if (inputPath == null || outputPath == null) {
            System.err.print("Could not read input and output path from arguments.\n Check you are passing them as arguments with --input and --output");
            return;
        }

        // Set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // Row has the shape 0 => VendorID, 1 => tpep_pickup_datetime, 2 => tpep_dropoff_datetime, 3 => passenger_count, 4 => RatecodeID
        final DataSet<Tuple5<Integer, String, String, Integer, Integer>> jfkAlarmsSubset = DatasetReader.getJFKAlarmsParameters(env, inputPath);

        // Get the trips in the expected output: (vendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count)
        final DataSet<Tuple4<Integer, String, String, Integer>> selected_trips_per_vendorID = jfkAlarmsSubset
                // First filter trips where destination is JFK and trips where there are two or more passengers
                .filter(row -> (row.f4 == 2) && (row.f3 >= 2))
                // Flat map to show trips per day. Output => VendorID, day-of-year, hour, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count
                .flatMap((Tuple5<Integer, String, String, Integer, Integer> row, Collector<Tuple6<Integer, String, String, String, String, Integer>> out) -> {
                    final String[] dateSplitted = row.f1.trim().split("[ :-]");
                    if (dateSplitted.length == 6) {
                        final String dayOfYear = dateSplitted[0] + "-" + dateSplitted[1] + "-" + dateSplitted[2];
                        final String hour = dateSplitted[3];
                        out.collect(new Tuple6<>(row.f0, dayOfYear, hour, row.f1, row.f2, row.f3));
                    }
                })
                .returns(Types.TUPLE(Types.INT, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.INT))
                // Group by vendorId, year-month-day and hour
                .groupBy(0, 1, 2)
                // sum passengerCount, getMin of pickup date and getMax of dropOff date
                .sum(5).andMin(3).andMax(4)
                // Only select vendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count for output
                .project(0, 3, 4, 5);


        // Write to output file
        selected_trips_per_vendorID.writeAsCsv(outputPath, FileSystem.WriteMode.OVERWRITE);

        // If verbose mode is activated execute is not necessary
        final Boolean verboseMode = false;

        // Show first 20 results sorted by vendorIDby console
        if (verboseMode) {
            try {
                selected_trips_per_vendorID.sortPartition(0, Order.ASCENDING).first(20).print();
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

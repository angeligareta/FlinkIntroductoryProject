package master2019.flink.YellowTaxiTrip;

import master2019.flink.YellowTaxiTrip.utils.DatasetReader;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;

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
 * Goal: Reports the vendors that do 5 or more trips during 3 hours that take at least 20
 * minutes.
 */
public class LargeTrips {
    public static void main(final String[] args) throws Exception {
        final String filePath = args[0];

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        final DataSet<Tuple3<Integer, String, String>> largeTripsDataset = DatasetReader.getLargeTripsParameters(env, filePath);
        //largeTripsDataset.first(10).print();


        // Trips that take at least 20 minutes
        largeTripsDataset.filter((Tuple3<Integer, String, String> row) -> {
            final Date pickupDate = DatasetReader.dateFormatter.parse(row.f1);
            final Date dropoffDate = DatasetReader.dateFormatter.parse(row.f2);
            final Long differenceInMilliseconds = dropoffDate.getTime() - pickupDate.getTime();
            final Integer differenceInMinutes = (int) ((differenceInMilliseconds / 1000) / 60);
            return (differenceInMinutes >= 20);
        }).first(10).print();

        try {
            env.execute();
        } catch (final Exception e) {
            e.printStackTrace();
        }
    }
}

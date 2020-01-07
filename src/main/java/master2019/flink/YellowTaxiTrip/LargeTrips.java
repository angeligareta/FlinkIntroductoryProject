package master2019.flink.YellowTaxiTrip;

import master2019.flink.YellowTaxiTrip.utils.DatasetReader;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;

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
	
	private final static String FILE_NAME = "largeTrips.csv";
	
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
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        
        // ID, initDate, endDate
        DataStream<Tuple3<Integer, String, String>> largeTripsDataset = DatasetReader.getLargeTripsParameters(env, inputPath);
        
        // Trips > 20 minutes
        DataStream<Tuple5<Integer, String, Integer, String, String>> selected_trips_per_vendorID = largeTripsDataset.filter(row -> {
        	
        	// Using calendar class avoids Data parser errors
        	Date initTime = dateStringToDate(row.f1);
			Date endTime = dateStringToDate(row.f2);
			
        	return endTime.getTime() - initTime.getTime() >= 20*60*1000;
        		
        })
		.assignTimestampsAndWatermarks(
				new AscendingTimestampExtractor<Tuple3<Integer, String, String>>() {
					@Override
					public long extractAscendingTimestamp(final Tuple3<Integer, String, String> row) {
						return dateStringToDate(row.f1).getTime();
					}
				}
				)
		.keyBy(0)
		.window(TumblingEventTimeWindows.of(Time.hours(3))) // Time window of 3 hours
		.apply(new WindowFunction<Tuple3<Integer, String, String>, Tuple5<Integer, Integer, String, String, String>, Tuple, TimeWindow>() {

			@Override
			public void apply(Tuple key, TimeWindow window, Iterable<Tuple3<Integer, String, String>> input,
					Collector<Tuple5<Integer, Integer, String, String, String>> out) throws Exception {
				
				Iterator<Tuple3<Integer, String, String>> iterator = input.iterator();
				Tuple3<Integer, String, String> first = iterator.next();
				Integer id = 0;
				Integer count = 0;
				String initDate = first.f1;
				String endDate = first.f2;
				if(first != null){
					id=first.f0;
					count = 1;
					
				}
				while(iterator.hasNext()){
					Tuple3<Integer, String, String> next = iterator.next();
					count++;
					endDate = first.f2;
				}
				String day = initDate.split(" ")[0]; 
				
				out.collect(new Tuple5<Integer, Integer, String, String, String>(id, count, day, initDate, endDate));				
			}
		})
		.filter(row -> row.f1 > 4).project(0, 2, 1, 3, 4); // 5 trips or more
        
        

/*
        final DataStream<Tuple4<Integer, Integer, String, String>> selected_trips_per_vendorID =
                largeTripsDataset

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
*/

        // Write to output file
        selected_trips_per_vendorID.writeAsCsv(outputPath+"/"+FILE_NAME, FileSystem.WriteMode.OVERWRITE);

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
    
    
	public static Date dateStringToDate(String dateString) {
		String[] initTimeArray = parseDateString(dateString);
		Integer minute = Integer.parseInt(initTimeArray[4]);
		Integer hour = Integer.parseInt(initTimeArray[3]);
		Integer day = Integer.parseInt(initTimeArray[2]);
		Integer month = Integer.parseInt(initTimeArray[1]);
		Integer year = Integer.parseInt(initTimeArray[0]);

		Calendar target = Calendar.getInstance();
		target.set(year, month, day, hour, minute);

		return target.getTime();

	}

	public static String[] parseDateString(String dateString) {

		String[] date = dateString.split(" ")[0].split("-");
		String[] time = dateString.split(" ")[1].split(":");

		return new String[] { date[0], date[1], date[2], time[0], time[1] };

	}
}

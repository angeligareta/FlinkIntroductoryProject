package master2019.flink.YellowTaxiTrip;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * In this class the JFK airport trips program has to be implemented.
 */
public class TestsWithFlink {
    public static void main(final String[] args) throws Exception {
        //1. set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //2. get input data
        final DataSet<String> text = env.fromElements(
                "To be, or not to be,--that is the question:--",
                "Whether 'tis nobler in the mind to suffer",
                "The slings and arrows of outrageous fortune",
                "Or to take arms against a sea of troubles,"
        );
        //3. Transformations
        final DataSet<Tuple2<String, Integer>> counts =
                // split up the lines in pairs (2-tuples) containing: (word,1)
                text.flatMap((String sentence, Collector<Tuple2<String, Integer>> out) -> {
                    for (String word : sentence.split("[ ,--:']")) {
                        if (!word.trim().isEmpty()) {
                            out.collect(new Tuple2<>(word, 1));
                        }
                    }
                }).returns(Types.TUPLE(Types.STRING, Types.INT))
                        .groupBy(0) // group by the tuple field "0"
                        .sum(1); // sum up tuple field "1"
        //4-5. Execute and print results
        counts.print();

        //                .reduceGroup((Iterable<Tuple6<Integer, String, String, String, String, Integer>> in, Collector<Tuple4<Integer, String, String, Integer>> out) -> {
//                    Integer vendorID = null;
//                    Date firstTrip = null;
//                    Date finalTrip = null;
//                    Integer total_passenger_count = 0;
//
//                    for (final Tuple6<Integer, String, String, String, String, Integer> row : in) {
//                        final Date pickupDate = DatasetReader.dateFormatter.parse(row.f3);
//                        final Date dropoffDate = DatasetReader.dateFormatter.parse(row.f4);
//
//                        if (firstTrip == null) {
//                            firstTrip = pickupDate;
//                            finalTrip = dropoffDate;
//                        } else {
//                            if (pickupDate.before(firstTrip)) {
//                                firstTrip = pickupDate;
//                            }
//                            if (dropoffDate.after(finalTrip)) {
//                                finalTrip = dropoffDate;
//                            }
//                        }
//
//                        if (vendorID == null) {
//                            vendorID = row.f0;
//                        } else {
//                            if (vendorID != row.f0) {
//                                System.out.println("WTF!");
//                            }
//                        }
//
//                        total_passenger_count += row.f5;
//                    }
//
//                    out.collect(new Tuple4<>(vendorID, DatasetReader.dateFormatter.format(firstTrip), DatasetReader.dateFormatter.format(finalTrip), total_passenger_count));
//                })
//                .returns(Types.TUPLE(Types.INT, Types.STRING, Types.STRING, Types.INT))
    }
}

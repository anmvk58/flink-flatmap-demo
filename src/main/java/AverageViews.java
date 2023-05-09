import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class AverageViews {
    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setGlobalJobParameters(parameterTool);

        DataStream<String> dataStream = StreamUtils.getDataStream(env, parameterTool);

        SingleOutputStreamOperator<Tuple2<String, Double>> averageViews = dataStream
                .map(new RowSplitter())
                .keyBy(0)
                .reduce(new SumAndCount())
                .map(new Average());


        averageViews.print();

        env.execute("Average Views");
    }

    private static class RowSplitter implements MapFunction<String, Tuple3<String, Double, Integer>> {
        @Override
        public Tuple3<String, Double, Integer> map(String sentence) throws Exception {
            String[] fields = sentence.split(",");
            if (fields.length == 2){
                return new Tuple3<>(fields[0], Double.parseDouble(fields[1]), 1);
            } else {
                return null;
            }
        }
    }

    private static class SumAndCount implements ReduceFunction<Tuple3<String, Double, Integer>> {
        @Override
        public Tuple3<String, Double, Integer> reduce(Tuple3<String, Double, Integer> nowValue, Tuple3<String, Double, Integer> newInput) throws Exception {
            return new Tuple3<>(
                    nowValue.f0,
                    nowValue.f1 + newInput.f1,
                    nowValue.f2 + newInput.f2
            );
        }
    }

    private static class Average implements MapFunction<Tuple3<String, Double, Integer>, Tuple2<String, Double>>{
        @Override
        public Tuple2<String, Double> map(Tuple3<String, Double, Integer> value) throws Exception {
            return new Tuple2<>(
                    value.f0,
                    value.f1/value.f2
            );
        }
    }
}

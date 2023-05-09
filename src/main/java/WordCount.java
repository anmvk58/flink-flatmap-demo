import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCount {
    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setGlobalJobParameters(parameterTool);

        DataStream<String> dataStream = StreamUtils.getDataStream(env, parameterTool);

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordDataStream = dataStream
                .flatMap(new WordCountSplitter())
                .keyBy(0)
                .sum(1);

        wordDataStream.print();

        env.execute("WordCount");
    }

    private static class WordCountSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> output) throws Exception {
            for (String word : sentence.split(" ")){
                output.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }
}

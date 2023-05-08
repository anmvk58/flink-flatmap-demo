import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Words {
    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setGlobalJobParameters(parameterTool);

        DataStream<String> dataStream;

        if (parameterTool.has("input")) {
            System.out.println("Executing example with file input");
            //read the text file from given input path
//            dataStream = env.readTextFile(parameterTool.get("input"));
            dataStream = env.readTextFile("D:\\Learning\\Flink\\FlinkFlatMapExample\\anmv.txt");
        } else if (parameterTool.has("host") && parameterTool.has("port")) {
            System.out.println("Executing example with socket stream");
            //read data from socket stream
            dataStream = env.socketTextStream(parameterTool.get("host"), Integer.parseInt(parameterTool.get("port")));
        } else {
            System.out.println("No input suitable for this example");
            System.exit(1);
            return;
        }

        DataStream<String> wordDataStream = dataStream.flatMap(new Splitter());

        wordDataStream.print();

        env.execute();
    }

    public static class Splitter implements FlatMapFunction<String, String> {
        @Override
        public void flatMap(String sentence, Collector<String> output) throws Exception {
            for (String word : sentence.split(" ")){
                output.collect(word);
            }
        }
    }
}


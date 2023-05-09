import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamUtils {
    public static DataStream<String> getDataStream(StreamExecutionEnvironment env, ParameterTool params){
        DataStream<String> dataStream = null;
        if (params.has("input")) {
            System.out.println("Executing example with file input");
            //  read the text file from given input path
            //  dataStream = env.readTextFile(parameterTool.get("input"));
            dataStream = env.readTextFile("D:\\Learning\\Flink\\FlinkFlatMapExample\\anmv.txt");
        } else if (params.has("host") && params.has("port")) {
            System.out.println("Executing example with socket stream");
            //read data from socket stream
            dataStream = env.socketTextStream(params.get("host"), Integer.parseInt(params.get("port")));
        } else {
            System.out.println("No input suitable for this example");
        }
        return dataStream;
    }
}

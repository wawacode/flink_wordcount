import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.typeinfo.Types;
public class MyWordCount {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lineds = env.readTextFile("f://data//words.txt");
        SingleOutputStreamOperator<Tuple2<String, Long>> wordstream = lineds.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            String[] words = line.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
    }
}).returns(Types.TUPLE(Types.STRING,Types.LONG));
        KeyedStream<Tuple2<String,Long>,String> groupds=wordstream.keyBy(ds->ds.f0);
        groupds.sum(1).print();
        env.execute();
    }
}

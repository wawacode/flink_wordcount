import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
public class SheWordCount {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lineds=env.socketTextStream("192.168.110.156",9000);
        lineds.flatMap((String line,Collector<Tuple2<String,Double>> out)->{
        String[] words=line.split(",");
        out.collect(Tuple2.of(words[0],Double.parseDouble(words[1])));
    }).returns(Types.TUPLE(Types.STRING,Types.DOUBLE))
            .keyBy(ds->ds.f0)
            .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .process(new SheAvgProcessFunction())
                .print();
        env.execute();
    }
}

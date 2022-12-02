import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.flink.table.api.Table;
import org.apache.flink.api.common.typeinfo.Types;
import static org.apache.flink.table.api.Expressions.$;
public class MyFlinkSQLWordCount {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings=EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv=StreamTableEnvironment.create(env,settings);
        DataStreamSource<String> lineds=env.readTextFile("f://data//words.txt");
        DataStream<Tuple2<String,Long>> dataStream=lineds.flatMap((String line,Collector<Tuple2<String,Long>> out)->{
            String[] words=line.split(" ");
            for(String word:words){
                out.collect(Tuple2.of(word,1L));
            }
        }).returns(Types.TUPLE(Types.STRING,Types.LONG));
        Table inputtable=tableEnv.fromDataStream(dataStream,$("word"),$("count"));
        Table resultTable=inputtable.groupBy($("word"))
                .select($("word"),$("count").sum());
        tableEnv.toRetractStream(resultTable,Types.TUPLE(Types.STRING,Types.LONG)).print();
        env.execute();
    }
}

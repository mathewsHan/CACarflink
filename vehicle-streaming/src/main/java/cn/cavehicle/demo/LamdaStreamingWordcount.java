package cn.cavehicle.demo;


import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
 
import java.util.Arrays;


/**
 * flink 1.12 只保留了
 *
 */
public class LamdaStreamingWordcount {
    public static void main(String[] args) throws Exception {
        //流和批的通用写法
        //建议模式设置再command-line中  $ bin/flink run -Dexecution.runtime-mode=BATCH <jarFile>
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStream = env.socketTextStream("node1", 9999).setParallelism(2);

        // env.setStateBackend();
        //lamda 表达式实现 flatmap逻辑
       /* dataStream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                return !StringUtils.isNullOrWhitespaceOnly(s);
            }
        })*/;
        SingleOutputStreamOperator<String> words = dataStream.flatMap((String line, Collector<String> out) ->
                Arrays.stream(line.split(" ")).forEach(out::collect)).returns(Types.STRING);
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAnOne = words.map(word -> Tuple2.of(word, 1)).returns(Types.TUPLE(Types.STRING, Types.INT));
        //分流
        KeyedStream<Tuple2<String, Integer>, Tuple> KeyedStream = wordAnOne.keyBy(0);

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = KeyedStream.sum(1);
        sum.print();


        // Interval Join
        // 流必须执行 execute();
         env.execute();




    }
}

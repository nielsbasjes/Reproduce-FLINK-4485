package com.bol.reproduce.FLINK4485;

import org.apache.flink.addons.hbase.TableInputFormat;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.PrintingOutputFormat;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;

import java.io.Serializable;

public class Main implements Serializable {
  public static void main(String[] args) throws Exception {
    Main main = new Main();
    main.run();
  }

  private int run() throws Exception {
    String directoryName = "test-FLINK-4485";
    String tableName = "test";

    Path directory = new Path(directoryName);
    FileSystem fs = directory.getFileSystem();
    fs.delete(directory, true);

    // Set up the execution environment
    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    // Get the raw results from HBase
    DataSet<String> resultDataSet = env
        .createInput(new HBaseMyTableInput(tableName))
      .map(new MapFunction<Tuple1<String>, String>() {
        @Override
        public String map(Tuple1<String> tuple) throws Exception {
          return tuple.f0;
        }
      });

    resultDataSet.output(new PrintingOutputFormat<String>());

    // execute program
    env.execute("Reproduce FLINK-4485");
    return 0;
  }

  class HBaseMyTableInput extends TableInputFormat<Tuple1<String>> {
    private String tableName;

    public HBaseMyTableInput(String tableName) {
      this.tableName = tableName;
    }

    @Override
    protected Scan getScanner() {
      Scan scanner = new Scan();
      scanner.setMaxVersions(1);
      scanner.setMaxResultSize(1);
      return scanner;
    }

    @Override
    protected String getTableName() {
      return tableName;
    }

    @Override
    protected Tuple1<String> mapResultToTuple(Result result) {
      return new Tuple1<>(new String(result.getRow()));
    }
  }
}

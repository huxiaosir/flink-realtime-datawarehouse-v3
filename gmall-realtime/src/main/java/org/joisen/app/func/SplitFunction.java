package org.joisen.app.func;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.joisen.utils.KeywordUtil;

import java.io.IOException;
import java.util.List;

/**
 * @Author Joisen
 * @Date 2023/2/7 16:48
 * @Version 1.0
 */
@FunctionHint(output = @DataTypeHint("ROW<word STRING, length INT>"))
public  class SplitFunction extends TableFunction<Row> {

    public void eval(String str) {
//        for (String s : str.split(" ")) {
//            // use collect(...) to emit a row
//            collect(Row.of(s, s.length()));
//        }
        List<String> list = null;
        try {
            list = KeywordUtil.splitKeyword(str);
            for (String word : list) {
                collect(Row.of(word));
            }
        } catch (IOException e) {
            collect(Row.of(str));
        }

    }
}

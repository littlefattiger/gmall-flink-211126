package com.atguigu.app.func;

import com.atguigu.utils.KeyWordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.List;

@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class SplitFunction extends TableFunction<Row> {

    public void eval(String str) {


        try {
            List<String> list = KeyWordUtil.splitKeyword(str);
            for (String word : list
            ) {
                collect(Row.of(word));
            }
        } catch (Exception e) {
            collect(Row.of(str));
        }

    }
}

package com.atguigu.utils;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class KeyWordUtil {

    public static List<String> splitKeyword(String text){
        List<String> keywordList = new ArrayList<>();
        StringReader reader = new StringReader(text);
        IKSegmenter ikSegmenter = new IKSegmenter(reader,false);
        try {
            Lexeme lexeme = null;
            while((lexeme = ikSegmenter.next())!=null){
                String keyword = lexeme.getLexemeText();
                keywordList.add(keyword);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return keywordList;
    }

    public static void main(String[] args) {
        System.out.println(splitKeyword("上硅谷大俗组之flink实施束苍"));
    }
}

package org.joisen.utils;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author Joisen
 * @Date 2023/2/7 16:38
 * @Version 1.0
 */
public class KeywordUtil {

    public static List<String> splitKeyword(String keyword) throws IOException {

        // 创建集合用于存放切分后的数据
        ArrayList<String> list = new ArrayList<>();

        // 创建IK分词对象
        StringReader reader = new StringReader(keyword);
        IKSegmenter ikSegmenter = new IKSegmenter(reader, false);

        // 取出切分好的词
        Lexeme next = ikSegmenter.next();
        while(next != null){
            String word = next.getLexemeText();
            list.add(word);
            next = ikSegmenter.next();
        }

        // 最终返回集合
        return list;
    }

    public static void main(String[] args) throws IOException {

        List<String> list = splitKeyword("尚硅谷大数据项目之flink实时数仓");
        System.out.println(list);

    }

}

package com.kunan.realtime.common.util;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;

public class IKUtil {
    public static void main(String[] args) throws IOException {
        String s = "映众（Inno3D）RTX 4070 OC12G 曜夜 映雪 GDDR6X 游戏电竞AI设计渲染设计电脑独立显卡 RTX4070 曜夜+显卡支架";
        StringReader stringReader = new StringReader(s);
        IKSegmenter ikSegmenter = new IKSegmenter(stringReader, true);
        Lexeme next = ikSegmenter.next();
        while (next != null){
            System.out.println(next.getLexemeText());
            next = ikSegmenter.next();
        }


    }
}

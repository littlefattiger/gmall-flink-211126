package com.atguigu.bean;

import lombok.Data;

@Data
public class TableProcess {
    String sourceTable;
    //输出表
    String sinkTable;
    //输出字段
    String sinkColumns;
    //主键字段
    String sinkPk;
    //建表扩展
    String sinkExtend;

}

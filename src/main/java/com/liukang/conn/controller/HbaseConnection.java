package com.liukang.conn.controller;

import com.liukang.conn.hbase.HBaseUtil;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.*;

@RestController
public class HbaseConnection  {
    @RequestMapping("hbase")
    public Object HbaseConn(){
       ResultScanner find_table = HBaseUtil.getScanner("FIND_TABLE", "125485624_20200312", "125485624_20200313");
        List<Map<String, Object>> rList = new ArrayList<>();
        for (Result result : find_table) {
            List valueList = new ArrayList();
            for (Cell cell : result.rawCells()) {
                // log.info("value值得：" + Bytes.toString(cell.getValue()));
                String[] split = Bytes.toString(cell.getValue()).split(",");
                valueList = Arrays.asList(split);
            }

            Map<String, Object> data = new HashMap<>();
            if (valueList.size() > 0) {
                for (int i = 0; i < valueList.size(); i++) {
                    data.put((String) valueList.get(i),valueList.get(i));
                }
            }
            rList.add(data);
        }
       return  rList;
    }
}

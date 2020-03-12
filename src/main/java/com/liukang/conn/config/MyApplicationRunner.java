package com.liukang.conn.config;


import com.liukang.conn.hbase.HBaseUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.util.*;

@Slf4j
@Component
public class MyApplicationRunner implements ApplicationRunner {

    @Override
    public void run(ApplicationArguments args) throws Exception {
        log.info("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@加载静态数据开始");
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
        log.info("MyApplicationRunner()--->系统首次加载维表数据rList.size()==="+rList.size());
        log.info("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@加载静态数据结束");
    }
}
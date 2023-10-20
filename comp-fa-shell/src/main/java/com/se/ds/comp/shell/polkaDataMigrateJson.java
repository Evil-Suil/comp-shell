package com.se.ds.comp.shell;

import com.alibaba.fastjson2.JSONObject;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @Author: Yelu Gu
 * @Date: 10/18/2023 10:52 AM
 */

@Slf4j
public class polkaDataMigrateJson {

    /**
     * pg配置数据
     */
    private static final String PG_URL = "jdbc:postgresql://172.26.196.242:5432/energymost";
    private static final String PG_USER = "emop_user";
    private static final String PG_PASSWORD = "P@ssw2rd";

    public static void main(String[] args) {
        Long startTime = System.currentTimeMillis();
        String fileName = "board-content.json";
        log.warn("脚本任务开始,startTime:{}", startTime);
        Integer successCount = 0;
        Integer failCount = 0;
        try {
            Connection connection = getPGConnection();
            InputStream inputStream = polkaDataMigrateJson.class.getClassLoader().getResourceAsStream(fileName);
            InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
            BufferedReader reader = new BufferedReader(inputStreamReader);
            String line;
            log.warn("插入任务开始startTime:{}", System.currentTimeMillis());
            while ((line = reader.readLine()) != null) {
                Long dataStartTime = System.currentTimeMillis();
                JSONObject jsonObject = JSONObject.from(JSONObject.parseObject(line).get("_v"));
                String id = jsonObject.getString("Id");
                String content = jsonObject.getString("Content");
                content = "null".equals(content) ? "{}" : content;
                if (insertPG(connection, id, content)) {
                    log.warn("插入成功,id:{},耗时:{}ms", id, System.currentTimeMillis() - dataStartTime);
                    successCount += 1;
                } else {
                    log.warn("插入失败,id:{},耗时:{}ms", id, System.currentTimeMillis() - dataStartTime);
                    failCount += 1;
                }
            }
            log.warn("插入完成,成功总数:{},耗时:{}ms", successCount, System.currentTimeMillis() - startTime);
            connection.close();
        } catch (Exception e) {
            log.error("流程错误,error:{}", e);
        } finally {
            log.warn("插入任务结束,successCount:{},failCount:{}", successCount, failCount);
            System.exit(0);
        }
    }

    private static Boolean insertPG(Connection connection, String id, String content) {
        String sql = "INSERT INTO board_content (id, content) VALUES (?, ?::jsonb)";
        PreparedStatement statement = null;
        boolean result = false;
        try {
            statement = connection.prepareStatement(sql);
            statement.setString(1, id);
            statement.setString(2, content);
            statement.executeUpdate();
            statement.close();
            result = true;
        } catch (SQLException e) {
            log.error("插入记录失败", e);
            try {
                statement.close();
            } catch (SQLException ex) {
                log.error("关闭statement失败: {}", ex);
            }
        }
        return result;
    }

    private static Connection getPGConnection() {
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(PG_URL, PG_USER, PG_PASSWORD);
        } catch (SQLException e) {
            log.warn("链接PostgreSQL异常:{}", e);
        }
        return connection;
    }
}

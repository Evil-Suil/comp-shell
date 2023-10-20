package com.se.ds.comp.shell;

import com.alibaba.fastjson2.JSONObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Objects;

/**
 * @Author: Yelu Gu
 * @Date: 10/18/2023 10:52 AM
 */

@Slf4j
public class polkaDataMigrate {

    /**
     * mongo配置数据
     */
    private static final String MONGO_CONNECTION_URL = "mongodb://rem:RWyvBmDtc4Kb@dds-8vb16993b958dd641.mongodb.zhangbei.rds.aliyuncs.com:3717,dds-8vb16993b958dd642.mongodb.zhangbei.rds.aliyuncs.com:3717/REMInformation?replicaSet=mgset-505735302";
    private static final String MONGO_DATABASE = "REMInformation";
    private static final String MONGO_COLLECTION = "board-content";
    /**
     * pg配置数据
     */
    private static final String PG_URL = "jdbc:postgresql://172.26.196.242:5432/energymost";
    private static final String PG_USER = "emop_user";
    private static final String PG_PASSWORD = "P@ssw2rd";


    public static void main(String[] args) {
        log.warn("脚本任务开始,startTime:{}", System.currentTimeMillis());
        Integer successCount = 0;
        Integer failCount = 0;
        try {
            Connection connection = getPGConnection();
            MongoClient mongoClient = getMongoClient();
            MongoDatabase database = mongoClient.getDatabase(MONGO_DATABASE);
            MongoCollection<Document> collection = database.getCollection(MONGO_COLLECTION);
            MongoCursor<Document> iterator = collection.find().iterator();
            Long totalNumber = collection.countDocuments();
            log.warn("插入任务开始,需要执行的数据量:{},startTime:{}", totalNumber, System.currentTimeMillis());
            Long startTime = System.currentTimeMillis();
            while (iterator.hasNext()){
                Document document = iterator.next();
                Document data = (Document) document.get("_v");
                String id = data.getString("Id");
                Object contentObj = data.get("Content");
                String content = Objects.isNull(contentObj) ? "{}" : JSONObject.toJSONString(contentObj);
                if (insertPG(connection, id, content)) {
                    log.warn("插入成功,id:{},耗时:{}ms", id, System.currentTimeMillis() - startTime);
                    successCount += 1;
                } else {
                    log.warn("插入失败,id:{},耗时:{}ms", id, System.currentTimeMillis() - startTime);
                    failCount += 1;
                }
            }
            log.warn("插入完成,成功总数:{},耗时:{}ms", successCount, System.currentTimeMillis() - startTime);
            connection.close();
            mongoClient.close();
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

    private static MongoClient getMongoClient() {
        return new MongoClient(new MongoClientURI(MONGO_CONNECTION_URL));
    }
}

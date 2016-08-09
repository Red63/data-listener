package com.retail.datahub.canal;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.retail.commons.dkafka.product.DefaultEventProduct;
import com.retail.datahub.base.EventBatchModel;
import com.retail.datahub.common.AbstractCanalClient;
import com.retail.datahub.exception.ProcessDataException;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Resource;
import java.util.List;

/**
 * 集群模式
 *
 * Created by red on 2016/3/11.
 */
public class ClusterCanalClient extends AbstractCanalClient {
    private static final Logger logger               = LoggerFactory.getLogger(ClusterCanalClient.class);
    private static final Logger errorLogger          = LoggerFactory.getLogger("msg-error");
    private static final Logger binlogLogger         = LoggerFactory.getLogger("binlog-msg");
    private static final Logger heartbeatLogger      = LoggerFactory.getLogger("heartbeat-msg");

    private String zkList;

    private String topic_prefix;
    private String topic_suffix;

    private DefaultEventProduct mysqlLogProduct;

    //用于对比
    private static String upJson = "";

    public ClusterCanalClient(){}

    public ClusterCanalClient(String destination){
        super(destination);
    }

    public void setZkList(String zkList) {
        this.zkList = zkList;
    }

    public void setTopic_prefix(String topic_prefix) {
        this.topic_prefix = topic_prefix;
    }

    public void setTopic_suffix(String topic_suffix) {
        this.topic_suffix = topic_suffix;
    }

    public void setMysqlLogProduct(DefaultEventProduct mysqlLogProduct) {
        this.mysqlLogProduct = mysqlLogProduct;
    }

    public void init() {
        // 基于固定canal server的地址，建立链接，其中一台server发生crash，可以支持failover
        // CanalConnector connector = CanalConnectors.newClusterConnector(
        // Arrays.asList(new InetSocketAddress(
        // AddressUtils.getHostIp(),
        // 11111)),
        // "stability_test", "", "");

        // 基于zookeeper动态获取canal server的地址，建立链接，其中一台server发生crash，可以支持failover
        CanalConnector connector = CanalConnectors.newClusterConnector(this.zkList, this.destination, "", "");

        //final ClusterCanalClient clientTest = new ClusterCanalClient(destination);
        final ClusterCanalClient _this = this;
        _this.setConnector(connector);
        _this.start();

        Runtime.getRuntime().addShutdownHook(new Thread() {

            public void run() {
                try {
                    logger.info("## stop the canal client");
                    _this.stop();
                } catch (Throwable e) {
                    logger.warn("##something goes wrong when stopping canal:\n{}", ExceptionUtils.getFullStackTrace(e));
                } finally {
                    logger.info("## canal client is down.");
                }
            }

        });
    }

    @Override
    public void processData(List<EventBatchModel> eventBatchModels) throws ProcessDataException {
        //TODO

        for(EventBatchModel eventBatchModel : eventBatchModels){
            String msg = "";
            String topic;
            if(StringUtils.isNotEmpty(topic_prefix)){
                topic = topic_prefix + "_" + eventBatchModel.getDbName();
            } else {
                topic = "binlog_" + eventBatchModel.getDbName();
            }

            topic = StringUtils.isNotEmpty(topic_suffix)?topic + "_" + topic_suffix:topic;


            try {
                msg = JSONObject.toJSONString(eventBatchModel, SerializerFeature.WriteMapNullValue);

                //检查msg数据是否重复,如果重复不处理
                if(!StringUtils.equals(msg, upJson)){
                    //心跳表另外处理
                    if(StringUtils.equals("heartbeat", eventBatchModel.getRealTableName())) {
                        heartbeatLogger.info(String.format("心跳表:%s.%s|%s", eventBatchModel.getDbName(),
                                eventBatchModel.getRealTableName(),
                                JSONObject.toJSONString(eventBatchModel.getRowData()[0].getAfterColumns())));
                    } else {
                        mysqlLogProduct.send(topic, msg);
                        binlogLogger.info(String.format("topic:%s,size:%d,msg:%s", topic, eventBatchModels.size(), msg));
                    }
                }
                upJson = msg;
            } catch (Exception e){
                upJson = "";
                errorLogger.error(String.format("send msg to topic [%s] error...", topic), e);
                errorLogger.info(String.format("unsent msg:%s", msg));

                throw new ProcessDataException(e);
            }

        }
    }

}

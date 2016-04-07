package com.retail.datahub.canal;

import com.alibaba.fastjson.JSON;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.retail.common.sdk.kafka.message.KafkaProducerFactory;
import com.retail.datahub.base.EventBatchModel;
import com.retail.datahub.common.AbstractCanalClient;
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
    private static final Logger MSG_LOG = LoggerFactory.getLogger("data_listener_msg");

    private String topic;

    private String zkserver;

    @Resource
    private KafkaProducerFactory kafkaProducerFactory;

    public ClusterCanalClient(){}

    public ClusterCanalClient(String destination){
        super(destination);
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setZkserver(String zkserver) {
        this.zkserver = zkserver;
    }

    public void init() {
        // 基于固定canal server的地址，建立链接，其中一台server发生crash，可以支持failover
        // CanalConnector connector = CanalConnectors.newClusterConnector(
        // Arrays.asList(new InetSocketAddress(
        // AddressUtils.getHostIp(),
        // 11111)),
        // "stability_test", "", "");

        // 基于zookeeper动态获取canal server的地址，建立链接，其中一台server发生crash，可以支持failover
        CanalConnector connector = CanalConnectors.newClusterConnector(this.zkserver, this.destination, this.username, this.password);

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
    public void processData(List<EventBatchModel> eventBatchModels) {
        try {
            if(eventBatchModels != null && eventBatchModels.size() > 0){
                for (EventBatchModel batchModel: eventBatchModels){
                    String msg = JSON.toJSONString(batchModel);
                    String desTopic = topic;
                    if(StringUtils.isEmpty(desTopic)){
                        desTopic = "canal_" + batchModel.getDbName() + "_topic";
                    }

                    kafkaProducerFactory.sendMessage(desTopic, msg);
                    MSG_LOG.info("send message to kafka,topic:" + desTopic + " ##:" + msg);
                    //Thread.sleep(1000);
                }
            }
        } catch (Exception e){
            MSG_LOG.error("sending exception", e);
        }
    }

}

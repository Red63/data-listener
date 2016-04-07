package com.retail.datahub.canal;

import java.net.InetSocketAddress;
import java.util.List;

import com.alibaba.fastjson.JSON;
import com.retail.common.sdk.kafka.message.KafkaProducerFactory;
import com.retail.datahub.base.EventBatchModel;
import com.retail.datahub.common.AbstractCanalClient;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Resource;


/**
 * 单机模式
 * 
 * Created by red on 2016/3/11.
 */
public class SimpleCanalClient extends AbstractCanalClient {
    private static final Logger MSG_LOG = LoggerFactory.getLogger("data_listener_msg");

    private String topic;

    @Resource
    private KafkaProducerFactory kafkaProducerFactory;

    public SimpleCanalClient(){}

    public SimpleCanalClient(String des){
        super(des);
    }


    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void init() {
        //logger.info("服务器:" + this.canalhost);
        // 根据ip，直接创建链接，无HA的功能
        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress(this.canalhost,
            this.port), this.destination, this.username, this.password);

        //final SimpleCanalClient clientTest = new SimpleCanalClient(destination);
        final SimpleCanalClient _this = this;
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
                String msg = "";
                for (EventBatchModel batchModel: eventBatchModels){
                    msg = JSON.toJSONString(batchModel);
                    String desTopic = topic;
                    if(StringUtils.isEmpty(desTopic)){
                        desTopic = "canal_" + batchModel.getDbName() + "_topic";
                    }

                    MSG_LOG.info("send message to kafka,topic:" + desTopic + " ##:" + msg);
                    kafkaProducerFactory.sendMessage(desTopic, msg);
                }
            }
        } catch (Exception e){
            MSG_LOG.error("sending exception", e);
        }
    }

}

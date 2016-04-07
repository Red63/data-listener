package com.retail.datahub.common;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.CanalEntry.*;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;
import com.retail.datahub.base.ColumnModel;
import com.retail.datahub.base.EventBatchModel;
import com.retail.datahub.base.RowDataModel;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by red on 2016/3/11.
 */
public abstract class AbstractCanalClient {

    protected final static Logger logger = LoggerFactory.getLogger(AbstractCanalClient.class);
    protected static final String SEP = SystemUtils.LINE_SEPARATOR;
    protected static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
    protected volatile boolean running = false;
    protected Thread.UncaughtExceptionHandler handler = new Thread.UncaughtExceptionHandler() {

        public void uncaughtException(Thread t, Throwable e) {
            logger.error("parse events has an error", e);
        }
    };
    protected Thread thread = null;
    protected CanalConnector connector;
    protected static String context_format = null;
    protected static String row_format = null;
    protected static String transaction_format = null;

    protected String canalhost;
    protected String canalport;
    protected String username;
    protected String password;
    protected int port;//11111
    protected String destination;
    protected String listenerdb;
    protected String listenertable;
    protected String event;

    static {
        context_format = SEP + "****************************************************" + SEP;
        context_format += "* Batch Id: [{}] ,count : [{}] , memsize : [{}] , Time : {}" + SEP;
        context_format += "* Start : [{}] " + SEP;
        context_format += "* End : [{}] " + SEP;
        context_format += "****************************************************" + SEP;

        row_format = SEP
                + "----------------> binlog[{}:{}] , name[{},{}] , eventType : {} , executeTime : {} , delay : {}ms"
                + SEP;

        transaction_format = SEP + "================> binlog[{}:{}] , executeTime : {} , delay : {}ms" + SEP;

    }

    public AbstractCanalClient() {
    }

    public AbstractCanalClient(String destination) {
        this(destination, null);
    }

    public AbstractCanalClient(String destination, CanalConnector connector) {
        this.destination = destination;
        this.connector = connector;
    }

    protected void start() {
        Assert.notNull(connector, "connector is null");
        thread = new Thread(new Runnable() {

            public void run() {
                process();
            }
        });

        thread.setUncaughtExceptionHandler(handler);
        thread.start();
        running = true;
    }

    protected void stop() {
        if (!running) {
            return;
        }
        running = false;
        if (thread != null) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                // ignore
            }
        }

        MDC.remove("destination");
    }


    protected void printSummary(Message message, long batchId, int size) {
        long memsize = 0;
        for (Entry entry : message.getEntries()) {
            memsize += entry.getHeader().getEventLength();
        }

        String startPosition = null;
        String endPosition = null;
        if (!CollectionUtils.isEmpty(message.getEntries())) {
            startPosition = buildPositionForDump(message.getEntries().get(0));
            endPosition = buildPositionForDump(message.getEntries().get(message.getEntries().size() - 1));
        }

        SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);
        logger.info(context_format, new Object[]{batchId, size, memsize, format.format(new Date()), startPosition,
                endPosition});
    }

    protected String buildPositionForDump(Entry entry) {
        long time = entry.getHeader().getExecuteTime();
        Date date = new Date(time);
        SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);
        return entry.getHeader().getLogfileName() + ":" + entry.getHeader().getLogfileOffset() + ":"
                + entry.getHeader().getExecuteTime() + "(" + format.format(date) + ")";
    }

    protected void printEntry(List<Entry> entrys) {
        for (Entry entry : entrys) {
            long executeTime = entry.getHeader().getExecuteTime();
            long delayTime = new Date().getTime() - executeTime;

            if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN || entry.getEntryType() == EntryType.TRANSACTIONEND) {
                if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN) {
                    TransactionBegin begin = null;
                    try {
                        begin = TransactionBegin.parseFrom(entry.getStoreValue());
                    } catch (InvalidProtocolBufferException e) {
                        throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
                    }
                    // 打印事务头信息，执行的线程id，事务耗时
                    logger.info(transaction_format,
                            new Object[]{entry.getHeader().getLogfileName(),
                                    String.valueOf(entry.getHeader().getLogfileOffset()),
                                    String.valueOf(entry.getHeader().getExecuteTime()), String.valueOf(delayTime)});
                    logger.info(" BEGIN ----> Thread id: {}", begin.getThreadId());
                } else if (entry.getEntryType() == EntryType.TRANSACTIONEND) {
                    TransactionEnd end = null;
                    try {
                        end = TransactionEnd.parseFrom(entry.getStoreValue());
                    } catch (InvalidProtocolBufferException e) {
                        throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
                    }
                    // 打印事务提交信息，事务id
                    logger.info("----------------\n");
                    logger.info(" END ----> transaction id: {}", end.getTransactionId());
                    logger.info(transaction_format,
                            new Object[]{entry.getHeader().getLogfileName(),
                                    String.valueOf(entry.getHeader().getLogfileOffset()),
                                    String.valueOf(entry.getHeader().getExecuteTime()), String.valueOf(delayTime)});
                }

                continue;
            }

            if (entry.getEntryType() == EntryType.ROWDATA) {
                RowChange rowChage = null;
                try {
                    rowChage = RowChange.parseFrom(entry.getStoreValue());
                } catch (Exception e) {
                    throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
                }

                EventType eventType = rowChage.getEventType();

                logger.info(row_format,
                        new Object[]{entry.getHeader().getLogfileName(),
                                String.valueOf(entry.getHeader().getLogfileOffset()), entry.getHeader().getSchemaName(),
                                entry.getHeader().getTableName(), eventType,
                                String.valueOf(entry.getHeader().getExecuteTime()), String.valueOf(delayTime)});

                if (eventType == EventType.QUERY || rowChage.getIsDdl()) {
                    logger.info(" sql ----> " + rowChage.getSql() + SEP);
                    continue;
                }

                for (RowData rowData : rowChage.getRowDatasList()) {
                    logger.info("do before data......");
                    printColumn(rowData.getBeforeColumnsList());
                    if (eventType == EventType.DELETE) {
                        logger.info("delete after data......");
                        printColumn(rowData.getBeforeColumnsList());
                    } else if (eventType == EventType.INSERT) {
                        logger.info("insert after data......");
                        printColumn(rowData.getAfterColumnsList());
                    } else {
                        logger.info("update after data......");
                        printColumn(rowData.getAfterColumnsList());
                    }
                }
            }
        }
    }

    protected List<EventBatchModel> fetchData(List<Entry> entrys) {
        List<EventBatchModel> eventBatchModels = new ArrayList<>();
        String db = "";
        String table = "";
        for (Entry entry : entrys) {
            //check db and table
            db = entry.getHeader().getSchemaName();
            table = entry.getHeader().getTableName();
            if(StringUtils.isEmpty(db)) continue;
            if(StringUtils.isEmpty(table)) continue;
            if(StringUtils.isNotEmpty(listenerdb) && listenerdb.indexOf(db) < 0){
                continue;
            } else {
                if(StringUtils.isNotEmpty(listenertable) && listenertable.indexOf(table) < 0) continue;
            }

            long executeTime = entry.getHeader().getExecuteTime();
            long delayTime = new Date().getTime() - executeTime;

            if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN || entry.getEntryType() == EntryType.TRANSACTIONEND) {
                if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN) {
                    TransactionBegin begin = null;
                    try {
                        begin = TransactionBegin.parseFrom(entry.getStoreValue());
                    } catch (InvalidProtocolBufferException e) {
                        throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
                    }
                    // 打印事务头信息，执行的线程id，事务耗时
                    logger.info(transaction_format,
                            new Object[]{entry.getHeader().getLogfileName(),
                                    String.valueOf(entry.getHeader().getLogfileOffset()),
                                    String.valueOf(entry.getHeader().getExecuteTime()), String.valueOf(delayTime)});
                    logger.info(" BEGIN ----> Thread id: {}", begin.getThreadId());
                } else if (entry.getEntryType() == EntryType.TRANSACTIONEND) {
                    TransactionEnd end = null;
                    try {
                        end = TransactionEnd.parseFrom(entry.getStoreValue());
                    } catch (InvalidProtocolBufferException e) {
                        throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
                    }
                    // 打印事务提交信息，事务id
                    logger.info("----------------\n");
                    logger.info(" END ----> transaction id: {}", end.getTransactionId());
                    logger.info(transaction_format,
                            new Object[]{entry.getHeader().getLogfileName(),
                                    String.valueOf(entry.getHeader().getLogfileOffset()),
                                    String.valueOf(entry.getHeader().getExecuteTime()), String.valueOf(delayTime)});
                }

                continue;
            }

            if (entry.getEntryType() == EntryType.ROWDATA) {
                RowChange rowChage = null;

                EventBatchModel eventBatchModel = new EventBatchModel();
                try {
                    rowChage = RowChange.parseFrom(entry.getStoreValue());

                } catch (Exception e) {
                    throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
                }

                EventType eventType = rowChage.getEventType();

                //check event
                if(StringUtils.isNotEmpty(event) && event.indexOf(eventType.name()) < 0) continue;

                eventBatchModel.setDbName(db);
                eventBatchModel.setRealTableName(table);
                eventBatchModel.setLogicTableName(table);
                eventBatchModel.setEventType(eventType.name());
                eventBatchModel.setExecutionTime(entry.getHeader().getExecuteTime());
                SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);
                eventBatchModel.setEventDate(format.format(new Date()));

                logger.info(row_format,
                        new Object[]{entry.getHeader().getLogfileName(),
                                String.valueOf(entry.getHeader().getLogfileOffset()), entry.getHeader().getSchemaName(),
                                entry.getHeader().getTableName(), eventType,
                                String.valueOf(entry.getHeader().getExecuteTime()), String.valueOf(delayTime)});

                if (eventType == EventType.QUERY || rowChage.getIsDdl()) {
                    //logger.info(" sql ----> " + rowChage.getSql() + SEP);
                    continue;
                }

                RowDataModel[] rowDataModels = new RowDataModel[rowChage.getRowDatasList().size()];

                for(int j = 0; j<rowChage.getRowDatasList().size(); j++){
                    RowData rowData = rowChage.getRowDatasList().get(j);
                    ColumnModel columnModel = new ColumnModel();

                    ColumnModel[] beforeColumns = getColumn(rowData.getBeforeColumnsList());
                    ColumnModel[] afterColumns = null;
                    RowDataModel rowDataModel = new RowDataModel();
                    rowDataModel.setBeforeColumns(beforeColumns);


                    if (eventType == EventType.DELETE) {
                        beforeColumns = getColumn(rowData.getBeforeColumnsList());
                        rowDataModel.setBeforeColumns(beforeColumns);
                    } else if (eventType == EventType.INSERT) {
                        afterColumns = getColumn(rowData.getAfterColumnsList());
                        rowDataModel.setAfterColumns(afterColumns);
                    } else {
                        afterColumns = getColumn(rowData.getAfterColumnsList());
                        rowDataModel.setAfterColumns(afterColumns);
                    }


                    rowDataModels[j] = rowDataModel;
                }

                eventBatchModel.setRowData(rowDataModels);

                eventBatchModels.add(eventBatchModel);
            }
        }

        return eventBatchModels;
    }

    protected ColumnModel[] getColumn(List<Column> columns) {
        if(columns == null || columns.size()==0) return null;
        ColumnModel[] columnModels = new ColumnModel[columns.size()];

        for (int i=0; i<columns.size(); i++){
            ColumnModel columnModel = new ColumnModel();
            Column column = columns.get(i);

            columnModel.setName(column.getName());
            columnModel.setValue(column.getValue());
            columnModel.setType(column.getMysqlType());
            columnModel.setUpdated(column.getUpdated());
            columnModel.setKey(column.getIsKey());
            columnModel.setNull(column.getIsNull());

            columnModels[i] = columnModel;
        }
        return columnModels;
    }

    protected void printColumn(List<Column> columns) {
        for (Column column : columns) {
            StringBuilder builder = new StringBuilder();
            builder.append(column.getName() + " : " + column.getValue());
            builder.append("    type=" + column.getMysqlType());
            if (column.getUpdated()) {
                builder.append("    update=" + column.getUpdated());
            }
            builder.append(SEP);
            logger.info(builder.toString());
        }
    }

    public void setConnector(CanalConnector connector) {
        this.connector = connector;
    }

    protected void process(){
        int batchSize = 5 * 1024;
        while (running) {
            try {
                MDC.put("destination", destination);
                connector.connect();
                connector.subscribe();
                while (running) {
                    Message message = connector.getWithoutAck(batchSize); // 获取指定数量的数据
                    long batchId = message.getId();
                    int size = message.getEntries().size();
                    if (batchId == -1 || size == 0) {
                        // try {
                        // Thread.sleep(1000);
                        // } catch (InterruptedException e) {
                        // }
                    } else {
                        //printSummary(message, batchId, size);
                        //printEntry(message.getEntries());

                        List<EventBatchModel> eventBatchModels = fetchData(message.getEntries());
                        if (eventBatchModels != null && eventBatchModels.size() >0 ){
                            processData(eventBatchModels);
                        }

                    }

                    connector.ack(batchId); // 提交确认
                    // connector.rollback(batchId); // 处理失败, 回滚数据
                }
            } catch (Exception e) {
                logger.error("process error!", e);
            } finally {
                connector.disconnect();
                MDC.remove("destination");
            }
        }
    }

    public void setCanalhost(String canalhost) {
        this.canalhost = canalhost;
    }

    public void setCanalport(String canalport) {
        if(StringUtils.isEmpty(canalport)){
            this.port = 11111;
        } else {
            this.port = Integer.valueOf(canalport);
        }
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public void setListenerdb(String listenerdb) {
        this.listenerdb = listenerdb;
    }

    public void setListenertable(String listenertable) {
        this.listenertable = listenertable;
    }

    public void setEvent(String event) {
        this.event = event;
    }

    public abstract void processData(List<EventBatchModel> eventBatchModels);

}

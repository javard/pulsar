package org.apache.pulsar.io.clickhouse;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;
import org.bouncycastle.asn1.cms.TimeStampTokenEvidence;
import org.json.JSONArray;
import org.json.JSONObject;
import ru.yandex.clickhouse.ClickHouseStatement;

import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.sql.Date;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static java.sql.Statement.EXECUTE_FAILED;
import static java.sql.Statement.SUCCESS_NO_INFO;


@Connector(
        name = "clickhouse",
        type = IOType.SINK,
        help = "A sink connector that sends pulsar messages to ClickHouse",
        configClass = ClickHouseSinkConfig.class
)
@Slf4j
public class ClickHouseSink implements Sink<byte[]> {

    private ClickHouseSinkConfig clickHouseSinkConfig;
    private Connection clickHouseConnection;
    private PreparedStatement stmt;
    private Utils.Table table;
    private int batchSize;
    private BlockingQueue<Record<byte[]>> batch;

    Map<String, Object> config;
    SinkContext sinkContext;


    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {

        this.config = config;
        this.sinkContext = sinkContext;

        log.info("Loading configuration..");
        clickHouseSinkConfig = ClickHouseSinkConfig.load(config);
        log.info("validating...");
        clickHouseSinkConfig.validate();

        this.batchSize = clickHouseSinkConfig.getBatchSize();
        batch = new ArrayBlockingQueue<>(batchSize);

        Class.forName(ClickHouseSinkConfig.JDBCDriver);

        log.info("Connecting to database...");
        int counter = 1;
        boolean success = false;
        while (!success){
            try {
                clickHouseConnection = clickHouseSinkConfig.getConnection();
                //clickHouseConnection = DriverManager.getConnection(clickHouseSinkConfig.getJdbcUrl());
                success = true;
            }catch (Exception e ){
                e.printStackTrace();
                counter = counter > 60 ? 60 : counter;
                int waitTime= counter*1000;
                log.info("Failed to connect to ClickHouse! Retrying in "+(waitTime/1000)+" seconds...");
                Thread.sleep(waitTime);
                counter++;
            }
        }

        table =  new Utils.Table(clickHouseSinkConfig.getTableName());

        log.info("Getting columns info..");
        getColumnsData();

        log.info("Preparing Statement...");
        stmt = getPreparedStatement();
    }

    @Override
    public void write(Record<byte[]> record)  {

        //log.info("CHEGOU: "+ new String(record.getValue(), StandardCharsets.UTF_8));
        try {
            batch.put(record); // waits if the queue is full
        }catch (Exception e){

        }


        synchronized (this){
            if( batch.size() == batchSize ){ // queue full

                List<Record<byte[]>> tempList= new ArrayList<>(); //copy of records to later send the acks
                Collection<Record<byte[]>> records = new ArrayList<>();
                batch.drainTo(records);

                for(Record<byte[]> r: records ){
                    tempList.add(r);

                    String recordValue = new String(r.getValue(), StandardCharsets.UTF_8);
                    JSONArray snapshot = new JSONArray(recordValue);

                    //log.info("Snap: " + snapshot.toString());
                    for(int i =0; i <snapshot.length(); i++){
                        JSONObject entry = snapshot.getJSONObject(i);
                        log.info("Adding to batch: " + entry.toString());
                        for (Utils.Column col: table.columns) {
                            setColumnValue(entry, col);
                        }
                        try {
                            stmt.addBatch();
                        }catch (Exception e){
                            e.printStackTrace();
                            log.info("Exception trying to add a line to the batch!");
                        }

                    }
                    System.out.println("Snapshot size: "+snapshot.length());
                }

                try{
                    //talvez desligar os commits e fazer depois, assim se o clickhouse for a baixo a meio não ficam so algumas linhas commited. VER ISTO!!
                    long start= System.currentTimeMillis();
                    int[] res = stmt.executeBatch();
                    System.out.println("Duration of insert: " +(System.currentTimeMillis() - start));
                    System.out.println(res.length);

                    for(int i=0; i< records.size(); i++){
                        System.out.println("res: "+res[i] );
                        if (res[i] != EXECUTE_FAILED){
                            tempList.get(i).ack();
                        }
                        else{
                            log.info("Failed to execute statement");
                            tempList.get(i).fail();
                        }
                    }
                    stmt.clearBatch();
                    stmt.close();

                }catch (Exception e){
                    log.info("EXCEPTION EXECUTING BATCH!");
                    for(int i=0; i<batchSize;i++ ){
                        tempList.get(i).fail();
                    }
                    checkConnection();
                    e.printStackTrace();
                }
            }
        }
    }

    @Override
    public void close() {
        if (clickHouseConnection != null){
            try {
                clickHouseConnection.close();
            }catch ( Exception e){
                e.printStackTrace();
                log.info("Exception trying to close clickhouse connection!");
            }
        }
    }

    private void checkConnection(){
        try {
            clickHouseSinkConfig.getConnection();
            //DriverManager.getConnection(clickHouseSinkConfig.getJdbcUrl());
        }catch (Exception e){
            log.info("Connection to clickhouse server failed.");
            close();
            try {
                open(this.config, this.sinkContext);
            }catch (Exception e2){
                e.printStackTrace();
                log.info("Failed to reopen the connection!");
            }
        }
    }

    private void setColumnValue(JSONObject newRow, Utils.Column col){
        try {
            int position = col.position;
            //log.info(col.name+" "+position);

            if(col.sqlType == Types.VARCHAR){
                stmt.setString(position, newRow.getString(col.mappedBy));
            }else if(col.sqlType == Types.INTEGER){
                stmt.setInt(position,newRow.getInt(col.mappedBy));
            }else if(col.sqlType == Types.BIGINT){
                stmt.setLong(position, newRow.getLong(col.mappedBy));
            }else if(col.sqlType == Types.FLOAT){
                stmt.setFloat(position, newRow.getFloat(col.mappedBy));
            }else if(col.sqlType == Types.DATE){
                stmt.setDate(position, Date.valueOf(newRow.getString(col.mappedBy)));
            }else if(col.sqlType == Types.TIMESTAMP){
                //log.info("TIMESTAMP: "+newRow.getString(col.mappedBy));
                stmt.setTimestamp(position, Timestamp.valueOf(newRow.getString(col.mappedBy)));
            }else if(col.sqlType == Types.TIME){
                stmt.setTime(position,Time.valueOf(newRow.getString(col.mappedBy)));
            }
        }catch (Exception e ){
            e.printStackTrace();
            log.info("Error setting column value! Column: "+col.name);
        }

    }

    /*
    Gets the predefined columns (defined in the config file) info from the database table
    */
    private Utils.Table getColumnsData() throws  Exception{
        Map<String, String> columnsMap = clickHouseSinkConfig.getColumnsMap();

        ResultSet rs = clickHouseConnection.getMetaData().getColumns(
                null,
                clickHouseSinkConfig.getSchemaName(),
                clickHouseSinkConfig.getTableName(),
                null);

        int counter =1;
        while (rs.next()) {
            final String columnName = rs.getString(4);
            final int sqlDataType = rs.getInt(5);
            final String typeName = rs.getString(6);
            //final int position = rs.getInt(17);

            String mappedBy = columnsMap.get(columnName); //null when there is no mapping in the config file

            log.info("COLUMN INFO: {} {} {} {} {}", columnName, sqlDataType, typeName, mappedBy, counter);

            if(mappedBy != null){
                //Only adds to the columns list if the column is in the mapping
                Utils.Column column = new Utils.Column(columnName, sqlDataType, typeName, mappedBy,counter);
                table.columns.add(column);
                counter++;
            }
        }
        return table;
    }

    private PreparedStatement getPreparedStatement() throws Exception{
        StringBuilder query = new StringBuilder("INSERT INTO " + clickHouseSinkConfig.getSchemaName()+"."+clickHouseSinkConfig.getTableName());

        query.append(" (");
        for(Utils.Column col: table.columns ){
            query.append(col.name+",");
        }

        query.deleteCharAt(query.length()-1);
        query.append(") ");
        query.append(" VALUES ");

        StringBuilder line = new StringBuilder("(");
        for(int j=0; j <table.columns.size(); j++){
            line.append(j==table.columns.size()-1 ? "?)" : "?,");
        }

        query.append(line);
        log.info("Prepared query: "+query.toString());
        return clickHouseConnection.prepareStatement(query.toString());
    }


}

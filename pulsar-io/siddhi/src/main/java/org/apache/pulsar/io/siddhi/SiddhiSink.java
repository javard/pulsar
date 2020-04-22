package org.apache.pulsar.io.siddhi;

import io.netty.channel.ChannelFutureListener;
import io.netty.util.concurrent.GenericFutureListener;
import io.siddhi.extension.io.tcp.transport.TCPNettyClient;
import io.netty.channel.ChannelFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;
import org.inferred.freebuilder.shaded.org.openjdk.tools.javac.jvm.Gen;
import org.slf4j.Logger;

import java.lang.management.OperatingSystemMXBean;
import java.nio.charset.StandardCharsets;
import java.util.Map;

@Connector(
        name = "siddhi",
        type = IOType.SINK,
        help = "A sink connector that sends pulsar messages to siddhi",
        configClass = SiddhiSinkConfig.class
)
@Slf4j
public class SiddhiSink implements Sink<byte[]> {

    private TCPNettyClient tcpNettyClient;
    private SiddhiSinkConfig siddhiSinkConfig;
    private SinkContext context;
    private Map<String,Object> config;

    public SiddhiSink(){ }

    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
        this.context = sinkContext;
        this.config = config;

        log.info("Open started");
        siddhiSinkConfig = SiddhiSinkConfig.load(config);
        siddhiSinkConfig.validate();

        log.info("Creating client");
        tcpNettyClient = new TCPNettyClient();
        int port = Integer.parseInt(siddhiSinkConfig.getSiddhiTCPServerPort());

        log.info("Trying to connect...");
        int counter = 1;
        boolean success = false;
        while (!success){
            try {
                tcpNettyClient.connect(siddhiSinkConfig.getSiddhiTCPServerURL(), port );
                success = true;
            }catch (Exception e ){
                counter = counter > 60 ? 60 : counter;
                int waitTime= counter*1000;
                log.info("Failed to connect to siddhi! Retrying in "+(waitTime/1000)+" seconds...");
                Thread.sleep(waitTime);
                counter++;
            }
        }
        log.info("Open success");
    }

    @Override
    public void write(Record<byte[]> record) throws Exception {
        String recordValue = new String(record.getValue(), StandardCharsets.UTF_8);

        ChannelFuture future = tcpNettyClient.send(siddhiSinkConfig.getSiddhiChannelID(), recordValue.getBytes(StandardCharsets.UTF_8));

        //problema com grande quantidade de msg/s. Os objetos são guardados para depois serem ack: GC overhead limit exceeded
        future.addListener( (ChannelFutureListener) f -> {
            if (f.isSuccess() ) {
                record.ack();
                //log.info("Send success");
            }else{
                record.fail();
                log.info("Send failed: "+f.cause().getMessage());
                checkConnection();
            }
        });

        //record.ack();
    }

    @Override
    public void close() throws Exception {
        if(tcpNettyClient != null) {
            tcpNettyClient.disconnect();
        }
    }

    private void checkConnection()throws Exception{
        try{
            tcpNettyClient.connect(siddhiSinkConfig.getSiddhiTCPServerURL(), Integer.parseInt(siddhiSinkConfig.getSiddhiTCPServerPort()) );
        }catch (Exception e ){
            log.info("Connection to siddhi failed! Retrying to connect...");
            close();
            open(this.config, this.context);
        }
    }

}



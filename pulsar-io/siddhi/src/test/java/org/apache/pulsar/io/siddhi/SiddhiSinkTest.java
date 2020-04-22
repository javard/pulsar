package org.apache.pulsar.io.siddhi;

import io.siddhi.extension.io.tcp.transport.TCPNettyClient;
import org.apache.pulsar.io.core.SinkContext;
import org.mockito.Mock;
import org.testng.IObjectFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.ObjectFactory;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.*;


public class SiddhiSinkTest {

    @Mock
    private TCPNettyClient tcpNettyClient;

    private SiddhiSinkConfig siddhiSinkConfig;

    @Mock
    private SinkContext context;

    private Map<String,Object> config;

    private SiddhiSink sink;

    @ObjectFactory
    public IObjectFactory getObjectFactory() {
        return new org.powermock.modules.testng.PowerMockObjectFactory();
    }

    @BeforeMethod
    public void setUp() {
        config = createMap();
        tcpNettyClient = mock(TCPNettyClient.class);
        context = mock(SinkContext.class);
        sink = new SiddhiSink();
    }

    @AfterMethod
    public void tearDown() throws Exception {
        sink.close();
        verify(tcpNettyClient, times(1)).disconnect();
    }

    /*@Test
    public void testOpen() throws Exception {
        sink.open(config, context);
    }*/

    public static Map<String, Object> createMap() {
        final Map<String, Object> map = new HashMap<>();
        map.put("siddhiTCPServerURL", "localhost");
        map.put("siddhiTCPServerPort", "9892");
        map.put("siddhiChannelID","channelID");
        return map;
    }

}

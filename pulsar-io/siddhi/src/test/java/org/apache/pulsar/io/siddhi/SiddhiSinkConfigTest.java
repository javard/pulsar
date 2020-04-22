package org.apache.pulsar.io.siddhi;

import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;

import static org.testng.Assert.assertEquals;

public class SiddhiSinkConfigTest {


    private static File getFile(String fileName) {
        return new File(SiddhiSinkConfigTest.class.getClassLoader().getResource(fileName).getFile());
    }


    @Test
    public void testYamlFile() throws IOException {
        final File yaml = getFile("siddhiSinkConfig.yaml");
        final SiddhiSinkConfig config = SiddhiSinkConfig.load(yaml.getAbsolutePath());

        assertEquals(config.getSiddhiTCPServerURL(), "localhost");
        assertEquals(config.getSiddhiTCPServerPort(), "9892");
        assertEquals(config.getSiddhiChannelID(), "channelID");
    }


}

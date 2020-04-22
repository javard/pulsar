package org.apache.pulsar.io.siddhi;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.io.core.annotations.FieldDoc;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

@Data
@Accessors(chain = true)
public class SiddhiSinkConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    @FieldDoc(
            required = true,
            defaultValue = "",
            help = "The siddhi tcp server url. The default tcp server is localhost"
    )
    private String siddhiTCPServerURL;

    @FieldDoc(
            required = true,
            defaultValue = "",
            help = "The siddhi tcp server port. The default tcp server port is 9892"
    )
    private String siddhiTCPServerPort;

    @FieldDoc(
            required = true,
            defaultValue = "",
            help = "The siddhi channel ID to connect to"
    )
    private String siddhiChannelID;


    public static SiddhiSinkConfig load(String yamlFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(new File(yamlFile), SiddhiSinkConfig.class);
    }

    public static SiddhiSinkConfig load(Map<String, Object> map) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(new ObjectMapper().writeValueAsString(map), SiddhiSinkConfig.class);
    }




    public void validate() throws Exception{
        if (StringUtils.isEmpty(getSiddhiChannelID()) ||
              StringUtils.isEmpty(getSiddhiTCPServerURL()) ||
              StringUtils.isEmpty(getSiddhiTCPServerPort())){

            throw new IllegalArgumentException("There are some required properties missing. ");
        }

    }


}

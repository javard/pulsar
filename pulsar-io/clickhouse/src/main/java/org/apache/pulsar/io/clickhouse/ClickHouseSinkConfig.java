package org.apache.pulsar.io.clickhouse;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.collect.ArrayListMultimap;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.pulsar.io.core.annotations.FieldDoc;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.*;

@Data
@Accessors(chain = true)
public class ClickHouseSinkConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    /*
        sink para cada tabela
        diferentes topicos podem ser escritos para a mesma tabela (usar exp regulares no --inputs)
        o mapeamento para as colunas do clikhouse possivelmente terá de fazer parte da configuração (maybe not)
        so inserts e devem ser em batch (batch size configuravel)
    */
    public static final String JDBCDriver = "ru.yandex.clickhouse.ClickHouseDriver";

    @FieldDoc(
            required = true,
            defaultValue = "",
            help = "The JDBC url to connect to"
    )
    private String jdbcUrl;

    @FieldDoc(
            required = false,
            defaultValue = "",
            help = "The database username"
    )
    private String username;

    @FieldDoc(
            required = false,
            defaultValue = "",
            help = "The password for the username"
    )
    private String password;

    @FieldDoc(
            required = true,
            defaultValue = "",
            help = "The name of the table this sink writes to"
    )
    private String tableName;

    @FieldDoc(
            required = true,
            defaultValue = "",
            help = "The name of the schema"
    )
    private String schemaName;

    @FieldDoc(
            required =  true,
            defaultValue = "",
            help = "The columns mapping. The keys represent the table columns and the values the corresponding message attributes "
    )
    private Map<String, String> columnsMap;

    @FieldDoc(
            required = false,
            defaultValue = "10000",
            help = "The size of the batch of rows to insert into the table"
    )
    private int batchSize;



    public static ClickHouseSinkConfig load(String yamlFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(new File(yamlFile), ClickHouseSinkConfig.class);
    }


    public static ClickHouseSinkConfig load(Map<String, Object> map) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(new ObjectMapper().writeValueAsString(map), ClickHouseSinkConfig.class);
    }

    public Connection getConnection() throws Exception{
        String url;
        if (getJdbcUrl().charAt(getJdbcUrl().length()-1) == '/'){
            url = getJdbcUrl()+getSchemaName();
        }else{
            url = getJdbcUrl()+"/"+getSchemaName();
        }

        Class.forName(JDBCDriver);
        Connection connection;
        Properties properties = new Properties();
        if (!StringUtils.isEmpty(getUsername())) {
            properties.put("user", getUsername());
            properties.put("password", getPassword());
            connection = DriverManager.getConnection(url, properties);
        }else{
            connection = DriverManager.getConnection(url);
        }

        return connection;
    }


    public void validate() throws Exception{
        // verificar tambem o mapeamento das colunas

        if (StringUtils.isEmpty(getJdbcUrl()) ||
                StringUtils.isEmpty(getTableName())){
            throw new IllegalArgumentException("There are some required properties missing. ");
        }

        Map<String, String> map = getColumnsMap();
        Set<String> set = new HashSet<>(new ArrayList<>(map.values()));

    }

}

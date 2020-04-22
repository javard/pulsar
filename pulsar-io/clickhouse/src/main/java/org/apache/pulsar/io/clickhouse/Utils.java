package org.apache.pulsar.io.clickhouse;

import java.util.ArrayList;
import java.util.List;

public class Utils {


    public static class Table{
        String name;
        List<Column> columns;

        public Table(String name){
            this.name = name;
            this.columns = new ArrayList<>();
        }

    }

    public static class Column{
        String name;
        int sqlType;
        String typeName;
        String mappedBy;
        int position;

        public Column(String name, int sqlType, String typeName, String mappedBy, int position){
            this.name = name;
            this.sqlType = sqlType;
            this.typeName = typeName;
            this.mappedBy = mappedBy;
            this.position = position;
        }
    }


}

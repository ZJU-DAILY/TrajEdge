package org.example.dht;

import java.io.Serializable;

/**
 * @author alecHe
 * @desc ...
 * @date 2024-01-10 16:06:38
 */
public class Request implements Serializable {
    private String header;
    private Object data;
    public static enum DataType{
        Point,
        Address,
        Filter,
        ID,
        Response,
        None
    }
    private DataType dataType;

    public Request(String header, Object data, DataType dataType) {
        this.header = header;
        this.data = data;
        this.dataType = dataType;
    }

    public String getHeader() {
        return header;
    }

    public void setHeader(String header) {
        this.header = header;
    }

    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }

    public DataType getDataType() {
        return dataType;
    }

    public void setDataType(DataType dataType) {
        this.dataType = dataType;
    }
}

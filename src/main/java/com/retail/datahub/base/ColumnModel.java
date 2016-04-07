package com.retail.datahub.base;

import java.io.Serializable;

/**
 * Created by red on 2016/3/11.
 */
public class ColumnModel implements Serializable{

    private String name;

    private String value;

    private String type;

    /**
     * 是否是主键
     */
    private boolean isKey;

    private boolean isNull;

    /**
     * 如果EventType=UPDATE,用于标识这个字段值是否有修改
     */
    private boolean isUpdated;

    public ColumnModel(){}

    public ColumnModel(String name, String value, String type, boolean isKey, boolean isNull, boolean isUpdated) {
        this.name = name;
        this.value = value;
        this.type = type;
        this.isKey = isKey;
        this.isNull = isNull;
        this.isUpdated = isUpdated;
    }

    public String getName() {
        return name;
    }

    public boolean isKey() {
        return isKey;
    }

    public void setKey(boolean key) {
        isKey = key;
    }

    public boolean isNull() {
        return isNull;
    }

    public void setNull(boolean aNull) {
        isNull = aNull;
    }

    public boolean isUpdated() {
        return isUpdated;
    }

    public void setUpdated(boolean updated) {
        isUpdated = updated;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}

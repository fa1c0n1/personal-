package com.apple.aml.stargate.app.pojo;

import lombok.Data;

@Data
public class MongoCalciteView {
    private String viewName;
    private String schemaId;
    private String collectionName;
    private String appendClause;
}

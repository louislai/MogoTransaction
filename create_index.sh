#!/usr/bin/env bash

mongo --port 21100 <<EOF
use cs4224;

db.getCollectionNames().forEach(function(collName) {
    db.runCommand({dropIndexes: collName, index: "*"});
});

db.getCollection("district-next-order-id").createIndex({ d_w_id: 1, d_id: 1 });
db.getCollection("warehouse-tax").createIndex({ w_id: "hashed" });
db.getCollection("warehouse").createIndex({ w_id: "hashed" });
db.getCollection("district").createIndex({ d_w_id: 1, d_id: 1 });
db.getCollection("district-next-undelivered-id").createIndex({ d_w_id: 1, d_id: 1 });
db.getCollection("customer").createIndex({ c_balance: -1 });
db.getCollection("customer").createIndex({ c_w_id: 1, c_d_id: 1, c_id: 1 });
db.getCollection("order-order-line").createIndex({ o_w_id: 1, o_d_id: 1, o_id: -1 });
db.getCollection("item").createIndex({ i_id: "hashed" });
db.getCollection("stock").createIndex({ s_w_id: 1, s_i_id: 1 });
EOF

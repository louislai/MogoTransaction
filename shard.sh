#!/usr/bin/env bash

mongo --port 21100 <<EOF
use cs4224;

sh.shardCollection("cs4224.district-next-order-id", { d_w_id: 1, d_id: 1 });
sh.shardCollection("cs4224.warehouse-tax", { w_id: "hashed" });
sh.shardCollection("cs4224.warehouse", { w_id: "hashed" });
sh.shardCollection("cs4224.district", { d_w_id: 1, d_id: 1 });
sh.shardCollection("cs4224.district-next-undelivered-id", { d_w_id: 1, d_id: 1 });
sh.shardCollection("cs4224.customer", { c_w_id: 1, c_d_id: 1, c_id: 1 });
sh.shardCollection("cs4224.order-order-line", { o_w_id: 1, o_d_id: 1, o_id: -1 });
sh.shardCollection("cs4224.item", { i_id: "hashed" });
sh.shardCollection("cs4224.stock", { s_w_id: 1, s_i_id: 1 });
EOF

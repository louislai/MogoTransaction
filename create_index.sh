#!/usr/bin/env bash

mongo 21100 <<EOF
use cs4224;
db.district_next_order_id.createIndex({ "d_w_id": 1 });
db.customer.createIndex({c_w_id: 1, c_d_id: 1, c_id: 1});
EOF
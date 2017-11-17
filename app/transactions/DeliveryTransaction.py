from Transaction import Transaction
from datetime import datetime
import decimal

# Delivery Transaction (Transaction #3)
# This transaction is used to process the delivery of the oldest yet-to-be-delivered order for each of the 10
# districts in a specified warehouse.

class DeliveryTransaction(Transaction):
    def execute(self, params):
        w_id = int(params['w_id'])
        carrier_id = int(params['carrier_id'])

        # Prepared Statements
        # self.get_smallest_order_number_query = self.session.prepare('db.district.find({d_w_id: "{}", '
        #                                                             'd_id: "?"}, { d_smallest_o_id: 1, _id: 0 })'
        #                                                             .format(w_id))
        # increment_smallest_order = self.session.prepare('db.district.update_one({d_w_id: "?", d_id: "?"}, '
        #                                                 '{$set:{ d_smallest_o_id: "?"}})')
        # self.get_customer_id_query = self.session.prepare('db.order_orderline.find({o_w_id: "{}", o_d_id: "?", '
        #                                                   'o_id: "?"}, {o_c_id: 1, _id: 0 })'.format(w_id))
        # self.update_order_query = self.session.prepare('db.order_orderline.update_one({o_id: "?", o_w_id: "{}", '
        #                                                'o_d_id: "?"}, {$set:{o_carrier_id: "?"}})'.format(w_id))
        # self.get_order_line_number_query = self.session.prepare('db.order_orderline.find({orderline.ol_o_id: "?", '
        #                                                         'oderline.ol_w_id: "{}", orderline.ol_d_id: "?"}, '
        #                                                         '{orderline.ol_number: 1, orderline.ol_amount: 1, '
        #                                                         '_id: 0})'.format(w_id))
        # self.update_order_line_query = self.session.prepare('db.order_orderline.update_one({orderline.ol_o_id: "?", '
        #                                                     'orderline.ol_w_id: "{}", orderline.ol_d_id: "?", '
        #                                                     'orderline.ol_number: "?"}, '
        #                                                     '{$set:{orderline.ol_delivery_d: "?"}})'.format(w_id))
        # self.get_customer_balance_delivery_query = self.session.prepare('db.customer.find({c_id: "?", c_w_id: "{}", '
        #                                                                 'c_d_id: "?"}, {c_balance: 1, '
        #                                                                 'c_delivery_cnt: 1, _id: 1})'.format(w_id))
        # self.update_customer_balance_delivery_query = self.session.prepare('db.customer.update_one({c_id: "?", c_w_id: "{}", '
        #                                                                    'c_d_id: "?"}, {$set:{c_balance: "?", '
        #                                                                    'c_delivery_cnt: "?"}})'.format(w_id))

        for d_id in range(1, 11):
            # order_info = self.get_smallest_order_number(num)
            # if order_info is None:
            #     continue
            # smallest_order_number = int(order_info[0])
            # customer_id = int(order_info[1])
            smallest_order_number = self.get_smallest_order_number(w_id, d_id)
            if not smallest_order_number:
                continue

            customer_id = self.get_customer_id(w_id, d_id, smallest_order_number)
            if not customer_id:
                continue

            customer_balance_delivery = self.get_customer_balance_delivery(w_id, customer_id, d_id)
            if not customer_balance_delivery:
                continue

            self.session['district-next-undelivered-id'].update({'d_w_id': w_id, 'd_id': d_id},
                                                                {'$set':
                                                                {'d_next_undelivered_id': int(smallest_order_number) + 1}})

            self.update_order(w_id, smallest_order_number, carrier_id, d_id)
            sum_order = 0.
            orderline_info = self.get_order_line_number(w_id, smallest_order_number, d_id)
            for index in orderline_info:
                sum_order += float(index.ol_amount)

            self.update_order_line(w_id, smallest_order_number, d_id)

            delivery_cnt = customer_balance_delivery.c_delivery_cnt
            current_balance = customer_balance_delivery.c_balance
            self.update_customer_balance_delivery(w_id, customer_id, sum_order, d_id, current_balance, delivery_cnt)

        print 'Delivery Transaction done'
        # print

    # Find the order with the smallest O_ID using W_ID and DISTRICT_NO from 1 to 10
    # and O_CARRIER_ID = -1
    def get_smallest_order_number(self, w_id, d_id):
        results = self.session['district-next-undelivered-id'].find({'d_w_id': w_id, 'd_id': d_id},
                                                                    {'d_next_undelivered_id': 1, '_id': 0})
        results = list(results)

        def get_info(doc):
            d = {'d_next_undelivered_id': doc['d_next_undelivered_id']}
            return self.objectify(d)

        smallest_order = [get_info(doc) for doc in results]

        if not smallest_order:
            print('Error in finding the next undelivered order')
            return

        # print(smallest_order[0].d_next_undelivered_id)
        return smallest_order[0].d_next_undelivered_id

    # Get the customer id of the smallest order id
    def get_customer_id(self, w_id, d_id, smallest_order_number):
        results = self.session['order-order-line'].find({'o_w_id': w_id, 'o_d_id': d_id, 'o_id': smallest_order_number},
                                                        {'o_c_id': 1, '_id': 0})
        results = list(results)

        def get_info(doc):
            d = {'c_id': doc['o_c_id']}
            return self.objectify(d)

        customer_id = [get_info(doc) for doc in results]

        if not customer_id:
            print 'Cannot find customer for order number', smallest_order_number
            return

        # print(customer_id[0].c_id)
        return customer_id[0].c_id

    # Update the O_CARRIER_ID with CARRIER_ID input
    def update_order(self, w_id, smallest_order_number, carrier_id, d_id):
        self.session['order-order-line'].update_one({'o_id': smallest_order_number,
                                                     'o_w_id': w_id, 'o_d_id': d_id},
                                                    {'$set': {'o_carrier_id': carrier_id}})

    # Find all the order-line numbers of each order
    def get_order_line_number(self, w_id, smallest_order_number, d_id):
        results = self.session['order-order-line'].find({'o_id': smallest_order_number,
                                                         'o_w_id': w_id,
                                                         'o_d_id': d_id})
                                                        # {'o_orderline.ol_number': 1,
                                                        #  'o_orderline.ol_amount': 1,
                                                        #  '_id': 0})
        result = results[0]

        def get_info(doc):
            d = {'o_orderlines': doc['o_orderlines']}
            return self.objectify(d)

        order_line_info = get_info(result)
        return order_line_info.o_orderlines

    # Update all the order-line in the order by setting OL_DELIVERY_D to the current date and time
    def update_order_line(self, w_id, smallest_order_number, d_id):
        # self.session.execute(self.update_order_line_query.bind(
        #     # [datetime.strptime(datetime.utcnow().isoformat(' '), '%Y-%m-%d %H:%M:%S.%f'),
        #     [datetime.utcnow(),
        #      smallest_order_number, num, ol_number]))
        self.session['order-order-line'].update({'o_w_id': w_id,
                                                 'o_d_id': d_id,
                                                 'o_id': smallest_order_number},
                                                {'$set': {'o_ol_delivery_d': datetime.utcnow()}})


    # Get the current balance of the customer
    def get_customer_balance_delivery(self, w_id, customer_id, d_id):
        results = self.session['customer'].find({'c_id': customer_id, 'c_w_id': w_id, 'c_d_id': d_id},
                                                {'c_balance': 1, 'c_delivery_cnt': 1, '_id': 0})
        results = list(results)

        def get_info(doc):
            d = {'c_balance': doc['c_balance'],
                 'c_delivery_cnt': doc['c_delivery_cnt']}
            return self.objectify(d)

        balance_delivery = [get_info(doc) for doc in results]
        return balance_delivery[0]

    # Increase the current customer balance with the value of the order
    def update_customer_balance_delivery(self, w_id, customer_id, sum_order, d_id, current_balance, delivery_cnt):
        self.session['customer'].update_one({'c_w_id': w_id, 'c_id': customer_id, 'c_d_id': d_id},
                                            {'$set': {'c_balance': float(sum_order) + float(current_balance),
                                                      'c_delivery_cnt': delivery_cnt}})


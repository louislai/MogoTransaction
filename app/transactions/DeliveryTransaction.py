from Transaction import Transaction
from datetime import datetime

# Delivery Transaction (Transaction #3)
# This transaction is used to process the delivery of the oldest yet-to-be-delivered order for each of the 10
# districts in a specified warehouse.

class DeliveryTransaction(Transaction):
    def execute(self, params):
        w_id = int(params['w_id'])
        carrier_id = int(params['carrier_id'])

        for d_id in range(1, 11):
            smallest_order_number = self.get_smallest_order_number(w_id, d_id)
            if not smallest_order_number:
                continue

            customer_id = self.get_customer_id(w_id, d_id, smallest_order_number)
            if not customer_id:
                continue

            customer_balance_delivery = self.get_customer_balance_delivery(w_id, customer_id, d_id)
            if not customer_balance_delivery:
                continue



            self.session['district-next-undelivered-id'].update_one({'d_w_id': w_id, 'd_id': d_id},
                                                                {'$set':
                                                                {'d_next_undelivered_id': int(smallest_order_number) + 1}})

            sum_order = self.get_order_total_ol_amount(w_id, d_id, smallest_order_number)

            self.update_order(w_id, smallest_order_number, carrier_id, d_id)

            delivery_cnt = customer_balance_delivery.c_delivery_cnt
            current_balance = customer_balance_delivery.c_balance
            self.update_customer_balance_delivery(w_id, customer_id, sum_order, d_id, current_balance, delivery_cnt)

        print 'Delivery Transaction done'

    # Find the order with the smallest O_ID using W_ID and DISTRICT_NO from 1 to 10
    # and O_CARRIER_ID = -1
    def get_smallest_order_number(self, w_id, d_id):
        result = self.session['district-next-undelivered-id'].find_one({'d_w_id': w_id, 'd_id': d_id},
                                                                    {'d_next_undelivered_id': 1, '_id': 0})

        if not result:
            print('Error in finding the next undelivered order')
            return

        return result['d_next_undelivered_id']

    # Get the customer id of the smallest order id
    def get_customer_id(self, w_id, d_id, smallest_order_number):
        result = self.session['order-order-line'].find_one({'o_w_id': w_id, 'o_d_id': d_id, 'o_id': smallest_order_number},
                                                        {'o_c_id': 1, '_id': 0})

        return result['o_c_id'] if result else None

    def get_order_total_ol_amount(self, w_id, d_id, o_id):
        return list(self.session['order-order-line'].aggregate(
            [{'$match': {'o_w_id': w_id, 'o_d_id': d_id, 'o_id': o_id }}, {'$unwind': '$o_orderlines'},
             {'$group': {'_id': None, 'ol_amount': {'$sum': '$o_orderlines.ol_amount' }}}]))[0]['ol_amount']

    # Update the O_CARRIER_ID with CARRIER_ID input
    def update_order(self, w_id, smallest_order_number, carrier_id, d_id):
        self.session['order-order-line'].update_one({'o_id': smallest_order_number,
                                                     'o_w_id': w_id, 'o_d_id': d_id},
                                                    {'$set': {'o_carrier_id': carrier_id, 'o_delivery_d': datetime.utcnow()}})


    # Get the current balance of the customer
    def get_customer_balance_delivery(self, w_id, customer_id, d_id):
        result = self.session['customer'].find_one({'c_id': customer_id, 'c_w_id': w_id, 'c_d_id': d_id},
                                                {'c_balance': 1, 'c_delivery_cnt': 1, '_id': 0})

        return self.objectify(result) if result else None

    # Increase the current customer balance with the value of the order
    def update_customer_balance_delivery(self, w_id, customer_id, sum_order, d_id, current_balance, delivery_cnt):
        self.session['customer'].update_one({'c_w_id': w_id, 'c_id': customer_id, 'c_d_id': d_id},
                                            {'$set': {'c_balance': float(sum_order) + float(current_balance),
                                                      'c_delivery_cnt': delivery_cnt + 1}})


from Transaction import Transaction
from datetime import datetime

# Order Status Transaction (Transaction #4)
# This transaction queries the status of the last order of a customer

class OrderStatusTransaction(Transaction):
    def execute(self, params):
        c_w_id = int(params['c_w_id'])
        c_d_id = int(params['c_d_id'])
        c_id = int(params['c_id'])

        customer_info = self.get_customer_info(c_w_id, c_d_id, c_id)
        # print customer_info
        customer_name_first = customer_info.c_first
        customer_name_middle = customer_info.c_middle
        customer_name_last = customer_info.c_last
        customer_balance = customer_info.c_balance

        last_order = self.get_last_order(c_w_id, c_d_id, c_id)
        # print last_order
        order_number = last_order.o_id
        entry_date = last_order.o_entry_d
        carrier = last_order.o_carrier_id

        order_line, date_time = self.get_order_line(c_w_id, c_d_id, order_number)

        print('Customer Info: ', customer_name_first, customer_name_middle, customer_name_last,\
            ' has balance of ', customer_balance)
        print('Last order info: ', order_number, entry_date, carrier)

        for index in order_line:
            # print 'what', index
            item_number = index.ol_i_id
            supply_warehouse = index.ol_supply_w_id
            quantity = index.ol_quantity
            total_price = index.ol_amount
            print('Order-line info: ', item_number, supply_warehouse, quantity, total_price, date_time)

    # Get the customer info using C_ID
    def get_customer_info(self, c_w_id, c_d_id, c_id):
        results = self.session['customer'].find({'c_w_id': c_w_id, 'c_d_id': c_d_id, 'c_id': c_id},
                                                {'c_first': 1, 'c_middle': 1, 'c_last': 1, 'c_balance': 1, '_id': 0})
        results = list(results)

        def get_info(doc):
            d = {'c_first': doc['c_first'],
                 'c_middle': doc['c_middle'],
                 'c_last': doc['c_last'],
                 'c_balance': doc['c_balance']}
            return self.objectify(d)

        customer_info = [get_info(doc) for doc in results]
        if not customer_info:
            print('Cannot find customer with w_id {} d_id {} c_id {}'.format(c_w_id, c_d_id, c_id))
            return
        return customer_info[0]

    # Get the last order info from the customer
    def get_last_order(self, c_w_id, c_d_id, c_id):
        results = self.session['order-order-line'].find({'o_w_id': c_w_id, 'o_d_id': c_d_id, 'o_c_id': c_id},
                                                        {'o_id': 1, 'o_entry_d': 1, 'o_carrier_id': 1, '_id': 0})
        results = list(results)

        def get_info(doc):
            d = {'o_id': doc['o_id'],
                 'o_entry_d': doc['o_entry_d'],
                 'o_carrier_id': doc['o_carrier_id']}
            return self.objectify(d)

        last_order_info = [get_info(doc) for doc in results]
        return last_order_info[0]

    # Get info of each item in the latest order
    def get_order_line(self, c_w_id, c_d_id, o_id):
        results = self.session['order-order-line'].find({'o_w_id': c_w_id, 'o_d_id': c_d_id, 'o_id': o_id})
                                                        # {'o_orderline.ol_i_id': 1, 'o_orderline.ol_supply_w_id': 1,
                                                        #  'o_orderline.ol_quanity': 1, 'o_orderline.ol_amount': 1,
                                                        #  'o_ol_delivery_d': 1, '_id': 0})
        result = results[0]
        # print(result)

        def get_delivery_d(doc):
            d = {'o_delivery_d': doc['o_delivery_d'],
                 'o_orderlines': doc['o_orderlines']}
            return self.objectify(d)

        delivery_result = get_delivery_d(result)

        o_delivery_d = delivery_result.o_delivery_d
        o_orderlines = delivery_result.o_orderlines
        # print(o_orderlines[0].ol_i_name)

        # def get_info(doc):
        #     d = {'ol_i_id': doc['orderline.ol_i_id'],
        #          'ol_supply_w_id': doc['ol_supply_w_id'],
        #          'ol_quantity': doc['ol_quantity'],
        #          'ol_amount': doc['ol_amount']}
        #     return self.objectify(d)
        #
        # order_line_info = [get_info(doc) for doc in o_orderlines]
        #
        # print(order_line_info[0].ol_i_id)
        return o_orderlines, o_delivery_d



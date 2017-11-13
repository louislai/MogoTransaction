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

        order_line = self.get_order_line(c_w_id, c_d_id, order_number)

        print 'Customer Info: ', customer_name_first, customer_name_middle, customer_name_last,\
            ' has balance of ', customer_balance
        print 'Last order info: ', order_number, entry_date, carrier

        for index in order_line:
            # print 'what', index
            item_number = index.ol_i_id
            supply_warehouse = index.ol_supply_w_id
            quantity = index.ol_quantity
            total_price = index.ol_amount
            date_time = index.ol_delivery_d
            print 'Order-line info: ', item_number, supply_warehouse, quantity, total_price, date_time

    # Get the customer info using C_ID
    def get_customer_info(self, c_w_id, c_d_id, c_id):
        result = self.session.execute('db.customer.find({c_w_id: "{}", c_d_id: "{}", c_id: "{}"}, '
                                      '{c_first: 1, c_middle: 1, c_last: 1, c_balance: 1, _id: 0})'
                                      .format(c_w_id, c_d_id, c_id))
        if not result:
            print 'Cannot find customer with w_id {} d_id {} c_id {}'.format(c_w_id, c_d_id, c_id)
            return
        return result[0]

    # Get the last order info from the customer
    def get_last_order(self, c_w_id, c_d_id, c_id):
        result = self.session.execute('db.order_orderline.find_one({o_w_id: "{}", o_d_id: "{}", '
                                      'o_c_id: "{}"}, {o_id: 1, o_entry_d: 1, o_carrier_id: 1, _id: 0})'
                                      .format(c_w_id, c_d_id, c_id))
        return result[0]

    # Get info of each item in the latest order
    def get_order_line(self, c_w_id, c_d_id, o_id):
        result = self.session.execute('db.order_orderline.find({orderline.ol_w_id: "{}", orderline.ol_d_id: "{}", '
                                      'orderline.ol_o_id: "{}"}, {orderline.ol_i_id: 1, orderline.ol_supply_w_id: 1, '
                                      'orderline.ol_quanity: 1, orderline.ol_amount: 1, orderline.ol_delivery_d: 1, _id: 0})'
                                      .format(c_w_id, c_d_id, o_id))
        return result



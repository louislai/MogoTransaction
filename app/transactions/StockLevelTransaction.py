from Transaction import Transaction

class StockLevelTransaction(Transaction):

    def execute(self, params):
        w_id = int(params['w_id'])
        d_id = int(params['d_id'])
        threshold = int(params['t'])
        num_last_orders = int(params['l'])

        next_order_id = self.get_next_order_id(w_id, d_id)
        last_l_item_ids = self.get_last_l_item_ids(w_id, d_id, next_order_id, num_last_orders)
        num_items = self.count_items_below_threshold(w_id, last_l_item_ids, threshold)
        print
        print 'Total Num Items where stock quantity below threshold:', num_items

    """set(int): set of item ids
     Get the item ids belonging to the last l orders of a (warehouse id, district id)
    """
    def get_last_l_item_ids(self, w_id, d_id, next_order_id, num_last_orders):
        results = self.session['order-order-line']\
            .find({ 'o_w_id': w_id, 'o_d_id': d_id, 'o_id': { '$gte': next_order_id - num_last_orders }},
                  { 'o_orderlines.ol_i_id': 1, '_id': 0 })
        results = [int(ol['ol_i_id']) for order in results for ol in order['o_orderlines']]
        return set(results)

    """int: count
     From a given set of item ids, count the number of items that is 
     below a given threshold for a particular warehouse id
    """
    def count_items_below_threshold(self, w_id, item_ids, threshold):

        count = 0

        for item_id in item_ids:
            result = self.session['stock'].find_one({ 's_w_id': 1, 's_i_id': 1 }, { '_id': 0, 's_quantity': 1 })['s_quantity']
            result = int(result)
            count += 1 if result < threshold else 0

        return count
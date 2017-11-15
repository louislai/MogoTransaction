class Transaction:
    def __init__(self, session):
        self.session = session

    # params passed as a dictionary
    def execute(self, params):
        pass

    """int: next order id
     Get next order id for a particular (warehouse_id, district_id) key
    """
    def get_next_order_id(self, w_id, d_id):
        result = self.session['district-next-order-id']\
            .find_one({ 'd_w_id': w_id, 'd_id': d_id }, { '_id': 0, 'd_next_o_id': 1 })['d_next_o_id']

        return int(result)

    """
    Get customer info with warehouse, district id
    """
    def get_customer_info(self, c_w_id, c_d_id, c_id):
        result = self.session.execute('select c_first, c_middle, c_last, c_balance from customer where'
                                      ' c_w_id = {}, c_d_id = {}, c_id = {}'.format(c_w_id, c_d_id, c_id))
        return result[0]

    def objectify(self, d):
        class obj(object):
            def __init__(self, d):
                for a, b in d.items():
                    if isinstance(b, (list, tuple)):
                        setattr(self, a, [obj(x) if isinstance(x, dict) else x for x in b])
                    else:
                        setattr(self, a, obj(b) if isinstance(b, dict) else b)
        return obj(d)
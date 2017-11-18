from Transaction import Transaction

class DatabaseStateTransaction(Transaction):
    def execute(self, params):
        join_cast_str = lambda lst: ' '.join(map(str, list(lst)))

        print '--------------------------------------------------------------------------------------------------'
        print 'Final Database State'
        print list(self.session['warehouse'].aggregate([{ '$group': { '_id': None, 'total': { '$sum': '$w_ytd' }}}]))[0]['total']
        print 'sum(d_ytd), sum(d_next_o_id) from district', \
            list(self.session['district'].aggregate([{'$group': {'_id': None, 'total': {'$sum': '$d_ytd'}}}]))[0]['total'],\
            list(self.session['district-next-order-id'].aggregate([{'$group': {'_id': None, 'total': {'$sum': '$d_next_o_id'}}}]))[0]['total']
        customer_result = list(self.session['customer']\
                               .aggregate([{ '$group': { '_id': None,
                                                         'c_balance': { '$sum': '$c_balance' },
                                                         'c_ytd_payment': { '$sum': '$c_ytd_payment' },
                                                         'c_payment_cnt': { '$sum': '$c_payment_cnt' },
                                                         'c_delivery_cnt': { '$sum': 'c_delivery_cnt' }}}]))[0]

        print 'sum(c_balance), sum(c_ytd_payment), sum(c_payment_cnt), sum(c_delivery_cnt) from customer',\
            customer_result['c_balance'], customer_result['c_ytd_payment'], customer_result['c_payment_cnt'],\
            customer_result['c_delivery_cnt']

        order_max_result = list(self.session['order-order-line']\
                                .aggregate([{ '$group': { '_id': None, 'o_id': { '$max': '$o_id' }, 'o_ol_cnt': { '$max': '$o_ol_cnt' }}}]))[0]
        print 'max(o_id), sum(o_ol_cnt) from order_', order_max_result['o_id'], order_max_result['o_ol_cnt']
        print 'sum(ol_amount), sum(ol_quantity) from order_line', \
            join_cast_str(self.get_orderline_stat())
        print 'sum(s_quantity), sum(s_ytd), sum(s_order_cnt), sum(s_remote_cnt) from stock', \
            join_cast_str(self.get_stock_stat())
        print '--------------------------------------------------------------------------------------------------'

    def get_orderline_stat(self):
        ol_amount = 0
        ol_quantity = 0
        for w_id in range(1, 17):
            for d_id in range(1, 11):
                result = list(self.session['order-order-line']\
                              .aggregate([{ '$match': { 'o_w_id': w_id, 'o_d_id': d_id }},
                                          { '$unwind': '$o_orderline' },
                                          { '$group': { '_id': None, 'ol_amount': { '$sum': '$o_orderline.ol_amount' },
                                                        'ol_quantity': { '$sum': '$o_orderline.ol_quantity' }}}]))
                if not result:
                    continue
                result = result[0]
                ol_quantity += int(result['ol_quantity'])
                ol_amount += int(result['ol_amount'])
        return ol_amount, ol_quantity


    def get_stock_stat(self):
        s_quantity = 0
        s_ytd = 0
        s_order_cnt = 0
        s_remote_cnt = 0
        for w_id in range(1, 17):
            result = list(self.session['stock'].\
                         aggregate([{ '$match': { 's_w_id': w_id }},
                                    { '$group': { '_id': None, 's_order_cnt': { '$sum': '$s_order_cnt' },
                                                  's_remote_cnt': { '$sum': '$s_remote_cnt' },
                                                  's_ytd': { '$sum': '$s_ytd' },
                                                  's_quantity': { '$sum': '$s_quantity' }}}]))
            if not result:
                continue
            result = result[0]
            s_quantity += int(result['s_quantity'])
            s_ytd += int(result['s_ytd'])
            s_order_cnt += int(result['s_order_cnt'])
            s_remote_cnt += int(result['s_remote_cnt'])
        return s_quantity, s_ytd, s_order_cnt, s_remote_cnt

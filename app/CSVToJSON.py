import argparse
import csv
import itertools
import json

from collections import deque
from StatsCollector import StatsCollector

class CSVToJSON:

    DEFAULT_DIRECTORY = "."
    DEFAULT_ROW_READ = 1000000000
    DEFAULT_OUT_DIR = "data"
    SEPARATOR = ","
    JOIN_CH = "-"

    def __init__(self, dir_path = DEFAULT_DIRECTORY, row_count = DEFAULT_ROW_READ, out_dir = DEFAULT_OUT_DIR):
        self.OUT_DIR = out_dir if out_dir else DEFAULT_OUT_DIR
        self.DIR_PATH = dir_path
        self.ROW_COUNT = row_count if row_count else self.DEFAULT_ROW_READ

        self.WAREHOUSE_FILE_PATH  = self.DIR_PATH + "/warehouse.csv"
        self.ORDER_FILE_PATH      = self.DIR_PATH + "/order.csv"
        self.DISTRICT_FILE_PATH   = self.DIR_PATH + "/district.csv"
        self.CUSTOMER_FILE_PATH   = self.DIR_PATH + "/customer.csv"
        self.ITEM_FILE_PATH       = self.DIR_PATH + "/item.csv"
        self.ORDER_LINE_FILE_PATH = self.DIR_PATH + "/order-line.csv"
        self.STOCK_FILE_PATH      = self.DIR_PATH + "/stock.csv"

        self.OUT_WAREHOUSE_FILE_PATH  = self.OUT_DIR + "/warehouse.json"
        self.OUT_DISTRICT_FILE_PATH   = self.OUT_DIR + "/district.json"
        self.OUT_CUSTOMER_FILE_PATH   = self.OUT_DIR + "/customer.json"
        self.OUT_ITEM_FILE_PATH       = self.OUT_DIR + "/item.json"
        self.OUT_ORDER_ORDER_LINE_FILE_PATH = self.OUT_DIR + "/order-order-line.json"
        self.OUT_STOCK_FILE_PATH      = self.OUT_DIR + "/stock.json"
        self.OUT_WAREHOUSE_TAX_FILE_PATH = self.OUT_DIR + "/warehouse-tax.json"
        self.OUT_DISTRICT_NEXT_ORDER_ID  = self.OUT_DIR + "/district-next-order-id.json"
        self.OUT_DISTRICT_TAX_DATA  = self.OUT_DIR + "/district-tax-data.json"
        self.OUT_DISTRICT_NEXT_UNDELIVERED_ID =                 \
            self.OUT_DIR + "/district-next-undelivered-id.json"


    def timemeasure(original_function):
        def new_function(*args, **kwargs):
            timer = StatsCollector.Timer()
            timer.start()
            original_function(*args, **kwargs)
            timer.finish()
            print "%s finished in %s s" % (original_function.__name__, timer.get_last())
        return new_function


    @timemeasure
    def load_warehouse_data(self, csv_file, out_file):
        reader = csv.reader(csv_file)
        out_data = []
        for line in itertools.islice(reader, self.ROW_COUNT):
            out_data.append({'w_id'      : int(line[0]),
                             'w_name'    : line[1],
                             'w_street_1': line[2],
                             'w_street_2': line[3],
                             'w_city'    : line[4],
                             'w_state'   : line[5],
                             'w_zip'     : line[6],
                             'w_ytd'     : float(line[8])})
        for row in out_data:
            json.dump(row, out_file)
            out_file.write('\n')


    @timemeasure
    def load_district_data(self, csv_file, out_file):
        reader = csv.reader(csv_file)
        out_data = []
        for line in itertools.islice(reader, self.ROW_COUNT):
            out_data.append({'d_w_id'    : int(line[0]),
                             'd_id'      : int(line[1]),
                             'd_name'    : line[2],
                             'd_street_1': line[3],
                             'd_street_2': line[4],
                             'd_city'    : line[5],
                             'd_state'   : line[6],
                             'd_zip'     : line[7],
                             'd_ytd'     : float(line[9])})
        for row in out_data:
            json.dump(row, out_file)
            out_file.write('\n')


    @timemeasure
    def load_customer_data(self, csv_file, out_file):
        reader = csv.reader(csv_file)
        out_data = []
        for line in itertools.islice(reader, self.ROW_COUNT):
            out_data.append({'c_w_id'        : int(line[0]),
                             'c_d_id'        : int(line[1]),
                             'c_id'          : int(line[2]),
                             'c_first'       : line[3],
                             'c_middle'      : line[4],
                             'c_last'        : line[5],
                             'c_street_1'    : line[6],
                             'c_street_2'    : line[7],
                             'c_city'        : line[8],
                             'c_state'       : line[9],
                             'c_zip'         : line[10],
                             'c_phone'       : line[11],
                             'c_since'       : line[12],
                             'c_credit'      : line[13],
                             'c_credit_lim'  : float(line[14]),
                             'c_discount'    : float(line[15]),
                             'c_balance'     : float(line[16]),
                             'c_ytd_payment' : float(line[17]),
                             'c_payment_cnt' : int(line[18]),
                             'c_delivery_cnt': int(line[19]),
                             'c_data'        : line[20]});
            c_w_id, c_d_id, c_id, c_first, c_middle, c_last =            \
                    line[0], line[1], line[2], line[3], line[4], line[5]
            self.c_map[self.JOIN_CH.join([c_w_id, c_d_id, c_id])] = (c_first, c_middle, c_last)

        for row in out_data:
            json.dump(row, out_file)
            out_file.write('\n')


    @timemeasure
    def load_item_data(self, csv_file, out_file):
        reader = csv.reader(csv_file)
        out_data = []
        for line in itertools.islice(reader, self.ROW_COUNT):
            out_data.append({'i_id'   : int(line[0]),
                             'i_name' : line[1],
                             'i_price': float(line[2]),
                             'i_im_id': int(line[3]),
                             'i_data' : line[4]})
            # Store i_name for order_order_line
            i_id, i_name, i_price = line[0], line[1], line[2]
            self.i_map[i_id] = (i_name, i_price)
        for row in out_data:
            json.dump(row, out_file)
            out_file.write('\n')


    def read_another_orderline(self, current_order, order_line_file):
        # Reading new orderline
        line = order_line_file.readline()
        if line:
            line = line[:-1]
            line = line.split(self.SEPARATOR)
            self.order_line_queue.append({'ol_w_id'       :int(line[0]),
                                          'ol_d_id'       :int(line[1]),
                                          'ol_o_id'       :int(line[2]),
                                          'ol_number'     :int(line[3]),
                                          'ol_i_id'       :int(line[4]),
                                          'ol_delivery_d' :line[5],
                                          'ol_amount'     :float(line[6]),
                                          'ol_supply_w_id':int(line[7]),
                                          'ol_quantity'   :float(line[8]),
                                          'ol_dist_info'  :line[9]});

        while (len(self.order_line_queue) > 0):
            order_line = self.order_line_queue[0]
            key = (int(order_line['ol_w_id']), int(order_line['ol_d_id']), int(order_line['ol_o_id']))

            if (current_order < key):
                return None
            elif (current_order == key):
                self.order_line_queue.popleft()
                return order_line
            else:
                self.order_line_queue.popleft()

        return None



    @timemeasure
    def load_order_orderline_data(self, order_file, order_line_file, out_file):

        # Assume that order_file is sorted in ascending order of (w_id, d_id, o_id)
        # Assume that order_line_file is sorted in ascending order of (w_id, d_id, o_id, o_number)
        line_count = self.ROW_COUNT
        while (line_count > 0):
            line = order_file.readline()
            if not line:
                break
            line_count -= 1

            # Remove new line char at the end of string
            line = line[:-1]
            line = line.split(self.SEPARATOR)

            # Extarct c_first, c_middle, c_last
            o_w_id, o_d_id, o_id, o_c_id = line[0], line[1], line[2], line[3]
            c_first, c_middle, c_last = self.c_map[self.JOIN_CH.join([o_w_id, o_d_id, o_c_id])]

            obj = {'o_w_id'      :int(line[0]),
                   'o_d_id'      :int(line[1]),
                   'o_id'        :int(line[2]),
                   'o_c_id'      :int(line[3]),
                   'o_carrier_id':int(line[4]),
                   'o_ol_cnt'    :float(line[5]),
                   'o_all_local' :float(line[6]),
                   'o_entry_d'   :line[7],
                   'o_c_first'   :c_first,
                   'o_c_middle'  :c_middle,
                   'o_c_last'    :c_last,
                   'o_delivery_d':None,
                   'o_orderlines':[]}

            # Reading order_line match this current order
            while (True):
                current_order = (int(o_w_id), int(o_d_id), int(o_id))
                order_line_obj = self.read_another_orderline(current_order, order_line_file)

                if not order_line_obj:
                    break;

                # Extract ol_delivery_d
                obj['o_delivery_d'] = order_line_obj['ol_delivery_d']
                del order_line_obj['ol_delivery_d']

                # Add to orderlines
                ol_i_id = order_line_obj['ol_i_id']
                order_line_obj['i_name'], i_price = self.i_map[str(ol_i_id)]
                obj['o_orderlines'].append(order_line_obj)

            json.dump(obj, out_file)
            out_file.write('\n')

    """
        Loads stock data
    """
    @timemeasure
    def load_stock_data(self, csv_file, out_file):
        reader = csv.reader(csv_file)
        out_data = []
        for line in itertools.islice(reader, self.ROW_COUNT):
            s_i_id = line[1]
            i_name, i_price = self.i_map[s_i_id]

            out_data.append({'s_w_id'      : int(line[0]),
                             's_i_id'      : int(line[1]),
                             's_quantity'  : float(line[2]),
                             's_ytd'       : float(line[3]),
                             's_order_cnt' : int(line[4]),
                             's_remote_cnt': int(line[5]),
                             's_dist_01'   : line[6],
                             's_dist_02'   : line[7],
                             's_dist_03'   : line[8],
                             's_dist_04'   : line[9],
                             's_dist_05'   : line[10],
                             's_dist_06'   : line[11],
                             's_dist_07'   : line[12],
                             's_dist_08'   : line[13],
                             's_dist_09'   : line[14],
                             's_dist_10'   : line[15],
                             's_data'      : line[16],
                             's_i_name'    : i_name,
                             's_i_price'   : float(i_price)})
        for row in out_data:
            json.dump(row, out_file)
            out_file.write('\n')


    """
        Loads district next order id data
    """
    @timemeasure
    def load_district_next_order_id_data(self, csv_file, out_file):
        reader = csv.reader(csv_file)
        out_data = []
        for line in itertools.islice(reader, self.ROW_COUNT):
            out_data.append({'d_w_id'     : int(line[0]),
                             'd_id'       : int(line[1]),
                             'd_next_o_id': int(line[10])})
        for row in out_data:
            json.dump(row, out_file)
            out_file.write('\n')


    """
        Loads warehouse tax data
    """
    @timemeasure
    def load_warehouse_tax_data(self, csv_file, out_file):
        reader = csv.reader(csv_file)
        out_data = []
        for line in itertools.islice(reader, self.ROW_COUNT):
            out_data.append({'w_id' : int(line[0]),
                             'w_tax': float(line[7])})
        for row in out_data:
            json.dump(row, out_file)
            out_file.write('\n')
        

    """
        Loads district next order id data
    """
    @timemeasure
    def load_district_tax_data(self, csv_file, out_file):
        reader = csv.reader(csv_file)
        out_data = []
        for line in itertools.islice(reader, self.ROW_COUNT):
            out_data.append({'d_w_id': int(line[0]),
                             'd_id'  : int(line[1]),
                             'd_tax' : float(line[8])})
        for row in out_data:
            json.dump(row, out_file)
            out_file.write('\n')


    """
        Load district_undelivered_id data
    """
    @timemeasure
    def load_district_next_undelivered_id(self, csv_file, out_file):
        reader = csv.reader(csv_file)
        out_data = []
        for line in itertools.islice(reader, self.ROW_COUNT):
            d_w_id, d_id = line[0], line[1]
            key = self.JOIN_CH.join((d_w_id, d_id))
            next_undelivered_id = (self.map_last_delivery[key] if key in self.map_last_delivery else 0) + 1

            out_data.append({'d_w_id': int(line[0]),
                             'd_id'  : int(line[1]),
                             'd_next_undelivered_id': next_undelivered_id});
        for row in out_data:
            json.dump(row, out_file)
            out_file.write('\n')


    @timemeasure
    def extract_last_delivery(self, csv_file):
        reader = csv.reader(csv_file)
        for line in itertools.islice(reader, self.ROW_COUNT):
            # Extract data for other methods
            o_w_id, o_d_id, o_carrier_id = line[0], line[1], int(line[4])
            if o_carrier_id > 0:
                key = self.JOIN_CH.join((o_w_id, o_d_id))
                last_delivery = self.map_last_delivery[key] if key in self.map_last_delivery else 0
                self.map_last_delivery[key] = max(last_delivery, int(line[2]))


    def execute(self):
        # Initialize map for denormalizing tables
        self.i_map, self.c_map, self.map_last_delivery = {}, {}, {}
        self.order_line_queue = deque()

        self.load_warehouse_data(
                open(self.WAREHOUSE_FILE_PATH, "r"),
                open(self.OUT_WAREHOUSE_FILE_PATH, "w"))
        self.load_district_data(
                open(self.DISTRICT_FILE_PATH, "r"),
                open(self.OUT_DISTRICT_FILE_PATH, "w"))
        self.load_customer_data(
                open(self.CUSTOMER_FILE_PATH, "r"),
                open(self.OUT_CUSTOMER_FILE_PATH, "w"))
        self.load_item_data(
                open(self.ITEM_FILE_PATH, "r"),
                open(self.OUT_ITEM_FILE_PATH, "w"))
        # Reset outfile before writing, Append each order to the file so that we don't have
        # to store in memory
        open(self.OUT_ORDER_ORDER_LINE_FILE_PATH, "w").close()
        self.load_order_orderline_data(
               open(self.ORDER_FILE_PATH),
               open(self.ORDER_LINE_FILE_PATH),
               open(self.OUT_ORDER_ORDER_LINE_FILE_PATH, "a"))
        self.extract_last_delivery(open(self.ORDER_FILE_PATH, "r"))
        self.load_stock_data(
                open(self.STOCK_FILE_PATH, "r"),
                open(self.OUT_STOCK_FILE_PATH, "w"))
        self.load_district_next_order_id_data(
                open(self.DISTRICT_FILE_PATH, "r"),
                open(self.OUT_DISTRICT_NEXT_ORDER_ID, "w"))
        self.load_warehouse_tax_data(
                open(self.WAREHOUSE_FILE_PATH),
                open(self.OUT_WAREHOUSE_TAX_FILE_PATH, "w"))
        self.load_district_tax_data(
                open(self.DISTRICT_FILE_PATH, "r"),
                open(self.OUT_DISTRICT_TAX_DATA, "w"))
        self.load_district_next_undelivered_id(
                open(self.DISTRICT_FILE_PATH, "r"),
                open(self.OUT_DISTRICT_NEXT_UNDELIVERED_ID, "w"))

if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    # List of arguments
    ap.add_argument("-p", "--path", required=True, help="Path to directory containing data")
    ap.add_argument("-c", "--count", required=False, help="Number of rows to be read")
    ap.add_argument("-o", "--output", required=False, help="Output directory")
    args = vars(ap.parse_args())

    # Getting arguments
    dir_path = args['path']
    row_count = int(args['count']) if args['count'] else None
    out_dir = args['output']

    converter = CSVToJSON(dir_path, row_count, out_dir)
    converter.execute()

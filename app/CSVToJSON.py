import argparse
import csv
import itertools
import json

from StatsCollector import StatsCollector

class CSVToJSON:

    DEFAULT_DIRECTORY = "."
    DEFAULT_ROW_READ = 1000000000
    DEFAULT_OUT_DIR = "data"

    def __init__(self, dir_path = DEFAULT_DIRECTORY, row_count = DEFAULT_ROW_READ, out_dir = DEFAULT_OUT_DIR):
        self.OUT_DIR = out_dir
        self.DIR_PATH = dir_path
        self.ROW_COUNT = row_count

        self.WAREHOUSE_FILE_PATH  = self.DIR_PATH + "/warehouse.csv"
        self.ORDER_FILE_PATH      = self.DIR_PATH + "/order.csv"
        self.DISTRICT_FILE_PATH   = self.DIR_PATH + "/district.csv"
        self.CUSTOMER_FILE_PATH   = self.DIR_PATH + "/customer.csv"
        self.ITEM_FILE_PATH       = self.DIR_PATH + "/item.csv"
        self.ORDER_LINE_FILE_PATH = self.DIR_PATH + "/order-line.csv"
        self.STOCK_FILE_PATH      = self.DIR_PATH + "/stock.csv"

        self.OUT_WAREHOUSE_FILE_PATH  = self.OUT_DIR + "/warehouse.csv"
        self.OUT_ORDER_FILE_PATH      = self.OUT_DIR + "/order.csv"
        self.OUT_DISTRICT_FILE_PATH   = self.OUT_DIR + "/district.csv"
        self.OUT_CUSTOMER_FILE_PATH   = self.OUT_DIR + "/customer.csv"
        self.OUT_ITEM_FILE_PATH       = self.OUT_DIR + "/item.csv"
        self.OUT_ORDER_LINE_FILE_PATH = self.OUT_DIR + "/order-line.csv"
        self.OUT_STOCK_FILE_PATH      = self.OUT_DIR + "/stock.csv"
        self.OUT_WAREHOUSE_TAX_FILE_PATH = self.OUT_DIR + "/warehouse-tax.csv"
        self.OUT_DISTRICT_NEXT_ORDER_ID  = self.OUT_DIR + "/district-next-order-id.csv"
        self.OUT_DISTRICT_NEXT_SMALLEST_ORDER_ID_DATA =           \
            self.OUT_DIR + "/district-next-smallest-order-id.csv"


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
            out_data.append({'w_id'      : line[0],
                             'w_name'    : line[1],
                             'w_street_1': line[2],
                             'w_street_2': line[3],
                             'w_city'    : line[4],
                             'w_state'   : line[5],
                             'w_zip'     : line[6],
                             'w_ytd'     : line[8]});
        json.dump(out_data, out_file);

    def execute(self):
        # Initialize map for denormalizing tables

        self.load_warehouse_data(
                open(self.WAREHOUSE_FILE_PATH),
                open(self.OUT_WAREHOUSE_FILE_PATH, "w"))
    

if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    # List of arguments
    ap.add_argument("-p", "--path", required=True, help="Path to directory containing data")
    ap.add_argument("-c", "--count", required=False, help="Number of rows to be read")
    ap.add_argument("-o", "--output", required=True, help="Output directory")
    args = vars(ap.parse_args())

    # Getting arguments
    dir_path = args['path']
    row_count = int(args['count']) if args['count'] else None
    out_dir = args['output']
    print row_count

    converter = CSVToJSON(dir_path, row_count, out_dir)
    converter.execute()

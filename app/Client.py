import sys
import argparse
import traceback

from StatsCollector import StatsCollector
from Parser import Parser
from pymongo import MongoClient
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern
from transactions.DummyTransaction import DummyTransaction
from transactions.NewOrderTransaction import NewOrderTransaction
from transactions.PaymentTransaction import PaymentTransaction
from transactions.DeliveryTransaction import DeliveryTransaction
from transactions.OrderStatusTransaction import OrderStatusTransaction
from transactions.PopularItemTransaction import PopularItemTransaction
from transactions.StockLevelTransaction import StockLevelTransaction
from transactions.TopBalanceTransaction import TopBalanceTransaction

class Client:

    def __init__(self, num_trans):
        # Inits parser and stats_collector
        self.stats_collector = StatsCollector()
        self.parser = Parser()
        self.num_trans = num_trans


    """ Executes a transaction given cassandra session, transaction type and
        transaction params.
    """
    def execute_transaction(self, session, transaction_type, transaction_params):
        transaction = DummyTransaction(session)

        if transaction_type == Parser.NEW_ORDER:
            transaction = NewOrderTransaction(session)

        elif transaction_type == Parser.PAYMENT:
            transaction = PaymentTransaction(session)

        elif transaction_type == Parser.DELIVERY:
            transaction = DeliveryTransaction(session)

        elif transaction_type == Parser.ORDER_STATUS:
            transaction = OrderStatusTransaction(session)

        elif transaction_type == Parser.STOCK_LEVEL:
            transaction = StockLevelTransaction(session)

        elif transaction_type == Parser.POPULAR_ITEM:
            transaction = PopularItemTransaction(session)
        elif transaction_type == Parser.TOP_BALANCE:
            transaction = TopBalanceTransaction(session)

        elif transaction_type == Parser.ORDER_LINE:
            return
        else:
            return

        try:
            transaction.execute(transaction_params)
        except Exception:
            print "Transaction could not be executed", traceback.format_exc()


    """ Initalize necessary objects, read and execute transaction.
    """
    def execute(self):
        # Connect to mongodb server
        client = MongoClient('localhost', 21100)
        session = client.get_database("cs4224", read_concern=read_concern, write_concern=write_concern)
        sys.stderr.write("Using concern type: %s\n" % concern_type)

        # Reading transactions line by line, parsing and execute
        while self.num_trans > 0:
            self.num_trans = self.num_trans - 1
            # Break if interrupted by user
            try:
                line = sys.stdin.readline().strip()
            except KeyboardInterrupt:
                break

            # Break on empty line
            if not line:
                break

            # Count transaction
            self.stats_collector.transactions.count()

            # Parsing transaction
            transaction_type = self.parser.get_transaction_type(line)
            extra_line_cnt = self.parser.get_transaction_extra_line_count(transaction_type, line)
            extra_lines = []
            for i in range(extra_line_cnt):
                extra_line = sys.stdin.readline().strip()
                extra_lines.append(extra_line)
            transaction_params = self.parser.parse(line, transaction_type, extra_lines)

            # Execute transaction and measure time
            self.stats_collector.transaction_timer.start()
            self.execute_transaction(session, transaction_type, transaction_params)
            self.stats_collector.transaction_timer.finish()

        self.output()

    """ Print out statistics collected during execution
    """
    def output(self):
        transaction_count = self.stats_collector.transactions.get_count()
        transaction_time = self.stats_collector.transaction_timer.get_total_time()
        sys.stderr.write("Number of transactions executed: %s\n" % transaction_count)
        sys.stderr.write("Total execution time: %s\n" % transaction_time)
        sys.stderr.write("Execution throughput: %s (xact/s)\n" % (transaction_count * 1.0 / transaction_time))

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    # List of arguments
    ap.add_argument("-n", "--number", required=False, help="Number of transactions to be executed")
    ap.add_argument("-t", "--type", required=False, help="Read/Write concern type")
    args = vars(ap.parse_args())

    num_trans = int(args['number']) if args['number'] else 1000000000

    # Consistency level: 1 / not supplied for "local" for the read_concern & "1" for write concern
    # not-1 for "majority" for both read and write concerns
    concern_type = int(args['type']) if args['type'] else 0
    if (concern_type == 1):
        read_concern = ReadConcern("majority")
        write_concern = WriteConcern("majority")
    else:
        read_concern = ReadConcern("local")
        write_concern = WriteConcern(1)

    print "Executing client"
    client = Client(num_trans)
    client.execute()

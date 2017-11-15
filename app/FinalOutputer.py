from pymongo import MongoClient
from transactions.DatabaseStateTransaction import DatabaseStateTransaction

class FinalOutputer:

    """ Print out final db states
    """
    def output(self):
        client = MongoClient('localhost', 21100)
        session = client.cs4224

        transaction = DatabaseStateTransaction(session)
        transaction.execute({})

if __name__ == "__main__":
    print "Printing final db states"
    outputer = FinalOutputer()
    outputer.output()

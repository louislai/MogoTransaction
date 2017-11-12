from Transaction import Transaction

class DummyTransaction(Transaction):
    def execute(self, params):
        print self.session.getCollectionNames
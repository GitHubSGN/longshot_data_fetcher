class bs(object):
    def __init__(self, name, anualized_factor):
        self.name = name
        self.anualized_factor = anualized_factor
        self.allocation = None

    def predict(self):
        self.allocation = {}

    def get_allocation(self):
        return self.allocation

    def get_strategyname(self):
        return self.name

    def get_anualized_factor(self):
        return self.anualized_factor

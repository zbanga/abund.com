"""
Interactive Brokers style commission.
"""
class PerShareWithMin(object):
    """
    Fixed cost per share with a minimum commission amount per trade.
    """

    def __init__(self, comm_per_share=0.01, comm_min=1.0):
        """
        comm_per_share parameter is the cost of a trade per-share. $0.01
        comm_min is the minimum commission amount per trade. $1.0
        """ 
        self.comm_per_share = float(comm_per_share)
        self.comm_min = float(comm_min)

        assert self.comm_per_share >= 0.0
        assert self.comm_min >= 0.0
        
    def __repr__(self):
        return "{class_name}(comm_per_share={comm_per_share}, comm_min={comm_min})".format(
            class_name=self.__class__.__name__,
            comm_per_share=self.comm_per_share,
            comm_min=self.comm_min)

    def calculate(self, transaction):
        """
        returns a tuple of:
        (per share commission, total transaction commission)
        """
        shares = float(transaction.amount)
        total = max(self.comm_min, abs(shares)*self.comm_per_share);
        cost_per_share = total/abs(shares)
        return cost_per_share, total

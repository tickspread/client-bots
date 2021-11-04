from typing import List


class DecisionMakerBase:
    def __init__(self, *args, **kwds):
        ...

    def updateExchOrder(self, exchOrder):
        ...

    def getTradingOrders(self) -> List:
        ...

    def updateFailedOrder(self, orderId: int):
        ...

    def getCancelOrders(self) -> List:
        ...






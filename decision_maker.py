from typing import List

class DecisionMakerBase:

    def updateExchOrder(self, exchOrder):
        ...

    def updateFailedOrder(self, orderId):
        ...

    def getTradingOrders(self) -> List:
        ...


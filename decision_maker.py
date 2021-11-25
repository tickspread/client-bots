from typing import List

class DecisionMakerBase:

    def updateExchOrder(self, exchOrder):
        ...

    def updateTradingOrderStatus(self, orderStatus):
        ...

    def getTradingOrders(self) -> List:
        ...


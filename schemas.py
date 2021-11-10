from collections import namedtuple

CancelOrder  = namedtuple( "CancelOrder", "client_order_id, market" )

ExecuteOrder = namedtuple( "ExecuteOrder", 
                           """
                                client_order_id
                                amount
                                price
                                leverage
                                market
                                side
                                type
                           """
)

def dispatchOrder(ts_api, order):


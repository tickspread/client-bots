from datetime import datetime, timedelta
from collections import defaultdict

globalSessionStart = datetime(2020, 1, 1)

def sessionIdGen(login):
    sessionOffset = int( (datetime.utcnow() - globalSessionStart).total_seconds() * 1000 )
    while True:
        yield sessionOffset
        sessionOffset += 1

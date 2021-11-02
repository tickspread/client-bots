from datetime import datetime, timedelta
from collections import defaultdict, deque
from datetime import datetime

globalSessionStart = datetime(2020, 1, 1)

def sessionIdGen():
    sessionOffset = int( (datetime.utcnow() - globalSessionStart).total_seconds() * 1000 )
    while True:
        yield sessionOffset
        sessionOffset += 1

class TimedWindow:
    def __init__(self, timewindow, *args, **kwds):
        self.timewindow = timewindow
        self.deque = deque(*args, **kwds)

    def append(self, o):
        now = datetime.utcnow()
        while self.deque and self.timewindow > now - self.deque[0][0]:
            self.deque.popleft()
        self.deque.append(now, o)

    def __len__(self):
        return len(self.deque)



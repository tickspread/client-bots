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
    def __init__(self, timewindow, logFunction=None, trigger=None, *args, **kwds):
        self.timewindow = timewindow
        self.deque = deque(*args, **kwds)
        self.logFunction = logFunction
        self.trigger = trigger

    def __len__(self):
        return len(self.deque)

    def extend(self, seq):
        for e in seq:
            self.append(e)

    def append(self, o, now=None):
        if now is None:
            now = datetime.utcnow()
        try:
            self.logFunction(o)
        except TypeError:
            pass
        while self.deque and self.timewindow > now - self.deque[0][0]:
            self.deque.popleft()
        self.deque.append((now, o))
        if self.trigger is not None:
            (count, f) = self.trigger
            if len(self) >= count:
                f()
                self.trigger = None
            

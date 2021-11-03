import multiprocessing as mp
import time
import signal
import queue
from itertools import count
import re

def f1(q):
    c = count()
    while True:
        n = next(c)
        msg = f"{f1.__name__} put {n=} on the queue"
        print(f"[putting on queue] {msg=}")
        q.put(msg)

        try:
            e = q.get_nowait()
        except queue.Empty:
            e = None
        print(f"{f1.__name__} read {e=} from the queue")
        time.sleep(0.1)

def f2(q):
    while True:
        msg = q.get()
        print( f"{f2.__name__} read {msg=}")
        m = re.search(r"n=(\d+)", msg)
        if m is not None and (n := int(m.group(1))) % 10 == 1:
            q.put(f"error {n=}")
        else:
            print(f"{f2.__name__} did not read anything useful")
        time.sleep(0.5)


if __name__ == "__main__":
    q = mp.Queue()
    p0 = mp.Process(target=f1, args=(q,))
    p1 = mp.Process(target=f2, args=(q,))
    p0.daemon = p1.daemon = True
    p0.start()
    p1.start()
    time.sleep(10)

    

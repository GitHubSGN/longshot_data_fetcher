import time
import logging
from contextlib import contextmanager

@contextmanager
def timer(name: str, log = False):
    s = time.time()
    yield
    elapsed = time.time() - s
    if log:
        logging.info(f'[{name}] {elapsed: .3f}sec')
    else:
        print(f'[{name}] {elapsed: .3f}sec')
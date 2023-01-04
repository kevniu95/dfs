import time
from typing import Callable, List
from collections import deque
import requests
from requests.models import Response
import pickle

class RequestLimiter():
    """
    Static save decorator
    """
    def _save(self, name = None, path = './data/rl/') -> None:
        if not name:
            name = self.base[self.base.find('.') + 1:]
        print("Saving RequestLimiter status to disk...")
        with open(path + name + '.p', 'wb') as handle:
            pickle.dump(self, handle, protocol=pickle.HIGHEST_PROTOCOL)

    def save(func):
        def wrapper(self, *args, **kwargs):
            result = func(self, *args, **kwargs)
            self._save()
            return result
        return wrapper

    """ Constructor """
    @save
    def __init__(self, base_link : str = None, 
                    interval : int = None, # in seconds
                    limit : int = None, 
                    load : str = None):
        if not (load and self._load(load)):
            print("Instantiating from constructor...")
            self.base : str = base_link
            self.interval : int = interval
            self.limit : int = limit
            self.accesses : List[float] = deque() # In last 60 seconds
        self._popAccesses()
        print(f"Initialized with {self.length} of {self.limit} entries filled")
        print()
        
    """
    Public Facing Methods
    """
    @save
    def get(self, request : Callable, link : str) -> Response:
        if self.base not in link:
            print("You haven't indicated that this is a rate limited site!")
            return
        self._popAccesses()
        if self.length >= self.limit:
            print("You're about to go over the limit, you'll have to wait!")
            return
        res = request(link)
        self._appendAccess()
        print(f"Size of current queue... {self.length}")
        return res
    
    @save
    def getQueue(self):
        self._popAccesses()
        return self.accesses

    @property
    def length(self):
        return len(self.accesses)

    """
    Private Methods
    """
    # Constructor Helpers
    def _load_handle_to_self(self, handle):
        b = pickle.load(handle)
        self.base = b.base
        self.limit = b.limit
        self.interval = b.interval
        self.accesses = b.accesses

    def _load(self, load) -> bool:
        try:
            with open(load, 'rb') as handle:
                self._load_handle_to_self(handle)
                print(f"Successfully loaded previous Rate Limiter info for {self.base}")
                return True
        except:
            return False
    
    # Queue editors
    def _popAccesses(self) -> None:
        while (self.length > 0) and (time.time() - self.accesses[0] > self.interval):
            a = self.accesses.popleft()

    def _appendAccess(self) -> None:
        print("Successfully processed append to queue...")
        self.accesses.append(time.time())

    # Print dunder methods
    def __str__(self):
        return f"Website: {self.base} / Limit: {self.limit } / Visits in last {self.interval}: {self.length}"
    
    def __repr__(self):
        return f"Website: {self.base} / Limit: {self.limit } / Visits in last {self.interval}: {self.length}"


if __name__ == '__main__':
    # BASE = 'https://www.basketball-reference.com'
    BASE = 'https://www.espn.com'
    # bases = {'summary_base' :BASE + '/leagues/NBA_2023.html',
    #                 'schedule_base' : BASE + '/leagues/NBA_%s_games-%s.html'}
    a = RequestLimiter(BASE, 
                    interval = 60, 
                    limit = 20, 
                    load = 'data/espn.com.p')
    
    for i in range(9):
        time.sleep(1)
        a.get(requests.get, 'https://www.espn.com')
        print(a)
        print()
    a.save()
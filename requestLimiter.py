import time
from typing import Callable, List
from collections import deque
import requests
from requests.models import Response
import pickle

class RequestLimiter():
    def __init__(self, base_link : str = None, 
                    interval : int = None, # in seconds
                    limit : int = None, 
                    load : str = None):
        if not (load and self._load(load)):
            self.base : str = base_link
            self.interval = interval
            self.limit : int = limit
            self.accesses : List[float] = deque() # In last 60 seconds
        self._popAccesses()

    def getQueue(self):
        self._popAccesses()
        return self.accesses

    def save(self, name = None, path = './data/') -> None:
        if not name:
            name = self.base[self.base.find('.') + 1:]
        with open(path + name + '.p', 'wb') as handle:
            pickle.dump(self, handle, protocol=pickle.HIGHEST_PROTOCOL)

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
        
    @property
    def length(self):
        return len(self.accesses)

    def _popAccesses(self) -> None:
        while (self.length > 0) and (time.time() - self.accesses[0] > self.interval):
            self.accesses.popleft()

    def _appendAccess(self) -> None:
        print("Successfully processed append to queue...")
        self.accesses.append(time.time())

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
        return res
    
    def __str__(self):
        return f"Website: {self.base} / Limit: {self.limit } / Visits in last {self.interval}: {len(self.accesses)}"
    
    def __repr__(self):
        return f"Website: {self.base} / Limit: {self.limit } / Visits in last {self.interval}: {len(self.accesses)}"


if __name__ == '__main__':
    BASE = 'https://www.basketball-reference.com'
    bases = {'summary_base' :BASE + '/leagues/NBA_2023.html',
                    'schedule_base' : BASE + '/leagues/NBA_%s_games-%s.html'}
    a = RequestLimiter(BASE, 
                    interval = 60, 
                    limit = 20, 
                    load = 'data/basketball-reference.com.p')
    
    for i in range(4):
        time.sleep(1)
        a.get(requests.get, bases['summary_base'])
        print(a)
    a.save()

    # a = RequestLimiter(base_link = 'https://www.espn.com', 
    #                     limit = 10,  
    #                     interval = 15)
    # # a.get(requests.get, 'https://nfl.com')
    # print(a)
    
    # for i in range(9):
    #     time.sleep(1)
    #     a.get(requests.get, 'https://www.espn.com')
    #     print(a)
    # a.save()

    # time.sleep(7)
    # print("")
    # print("moving to a new object...")
    # b = RequestLimiter(load= 'data/espn.com.p')
    # for i in range(10):
    #     time.sleep(0.25)
    #     b.get(requests.get, 'https://www.espn.com')
    #     print(b)


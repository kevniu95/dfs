import time
from typing import Callable, List
from collections import deque
import requests
from requests.models import Response
import pickle

# class BatchRequestLimiter(RequestLimiter):
#     """
#     A request limiter that can handle a batch of requests larger than its limit
#     """
#     def __init__(self):



class RequestLimiter():
    """
    i. Static save decorator
        -Before Constructor
    """
    def _save(self, name = None, path = './data/rl/') -> None:
        if not name:
            name = self.base[self.base.find('.') + 1:]
        print("Saving RequestLimiter status to disk...")
        with open(path + name + '.p', 'wb') as handle:
            pickle.dump(self, handle, protocol=pickle.HIGHEST_PROTOCOL)

    def save(func):
        print("In save function...")
        def wrapper(self, *args, **kwargs):
            result = func(self, *args, **kwargs)
            self._save()
            return result
        return wrapper

    def popAccesses(func):
        def wrapper(self, *args, **kwargs):
            self._popAccesses()
            result = func(self, *args, **kwargs)
            return result
        return wrapper
    
            
    """ 
    A. Constructor
    """
    @save
    def __init__(self, base_link : str = None, 
                    interval : int = None, # in seconds
                    limit : int = None, 
                    load : str = None):
        print("In constructor...")
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
    B. Public Facing Methods
    """
    @popAccesses
    @save
    def get(self, link : str, waitForPop : bool = False, request : Callable = requests.get) -> Response:
        """
        waitForPop : bool 
            - If set to True, will make block on time needed for Pop of first item in queue
        """
        print(waitForPop)
        if self.base not in link:
            print("You haven't indicated that this is a rate limited site!")
            return
        if self.full and not waitForPop:
            print("You're about to go over the limit, you'll have to try again later.")
            return
        if self.full and waitForPop:
            print("You're about to go over the limit. Because you specified waitForPop, will wait for"\
                    " front of queue to pop.")
            self._wait_for_pop_time()
        res = request(link)
        self._appendAccess()
        print(f"Size of current queue... {self.length}")
        return res
    
    @popAccesses
    @save
    def getQueue(self) -> List[float]:
        return self.accesses

    @property
    def full(self) -> bool:
        return self.length >= self.limit

    @property
    def length(self) -> int:
        return len(self.accesses)

    """
    C. Private Methods
    """
    def _wait_for_pop_time(self) -> None:
        pop_time : float= self._get_pop_time()
        print(f"I'm going to sleep for {pop_time} - see ya!")
        time.sleep(pop_time + 2)
        print(f"Wow, just woke up after sleeping for {pop_time} - feeling refreshed!")
        self._popAccesses()
        return
    
    @popAccesses
    def _get_pop_time(self) -> float:
        if self.length == 0:
            return 0
        time_elapsed : float = time.time() - self.accesses[0]
        time_to_go : float = self.interval - time_elapsed
        return time_to_go

    # Constructor Helpers
    def _load_handle_to_self(self, handle : str) -> None:
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
    @popAccesses
    def __str__(self):
        return f"Website: {self.base} / Limit: {self.limit } / "\
                f"Visits in last {self.interval}: {self.length}"
    
    @popAccesses
    def __repr__(self):
        return f"Website: {self.base} / Limit: {self.limit } / "\
                f"Visits in last {self.interval}: {self.length}"


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
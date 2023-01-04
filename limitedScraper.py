from requestLimiter import RequestLimiter
from typing import Dict, Any, Callable
import pickle
import time
import requests

class LimitedScraper():
    def __init__(self, fx : Callable, name: str, linkDict: Dict[str, str], rl : RequestLimiter):
        self.fx = fx
        self.name = name
        self.state_dict = linkDict
        # self.stateDict = self._instStateDict(linkDict)
        self.rl = rl

    def _save(self, path = './data/ls/'):
        self.rl._save()
        with open(path + self.name + '.p', 'wb') as handle:
            pickle.dump(self, handle, protocol=pickle.HIGHEST_PROTOCOL)
    
    def save(func):
        def wrapper(self, *args, **kwargs):
            result = func(self, *args, **kwargs)
            self._save()
            return result
        return wrapper




if __name__ == '__main__':
    # BASE = 'https://www.basketball-reference.com'
    BASE = 'https://www.espn.com'
    # bases = {'summary_base' :BASE + '/leagues/NBA_2023.html',
    #                 'schedule_base' : BASE + '/leagues/NBA_%s_games-%s.html'}
    link_dict = {'abc' : 'https://www.espn.com'}
    a = LimitedScraper(linkDict = link_dict,
                    base_link = BASE, 
                    interval = 60, 
                    limit = 20, 
                    load = 'data/espn.com.p')
    print(a.linkDict)
    # for i in range(9):
    #     time.sleep(1)
    #     a.get(requests.get, 'https://www.espn.com')
    #     print(a)
    #     print()
    # a.save()
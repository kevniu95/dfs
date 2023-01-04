from requestLimiter import RequestLimiter
from typing import Dict, Any
import pickle
import time
import requests

class LimitedScraper(RequestLimiter):
    def __init__(self, name : str, linkDict : Dict[str, str], **kwargs):
        print(kwargs)
        super().__init__(**kwargs)
        self.name = name
        self.stateDict = self._instStateDict(linkDict)
        

    def _instStateDict(self, linkDict):
        new_dict = {}
        for k, v in linkDict.items():
            new_dict[k] = {'link' : v, 'done' : False}
        return new_dict
        
    def _save(self, path = './data/ls/'):
        super()._save()
        print("Saving LimitedScraper status to disk...")
        with open(path + self.name + '.p', 'wb') as handle:
            pickle.dump(self, handle, protocol=pickle.HIGHEST_PROTOCOL)



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
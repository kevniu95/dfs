import time
import requests
from requestLimiter import RequestLimiter

rl = RequestLimiter(base_link = 'https://www.espn.com' ,
                    interval = 60, # in seconds
                    limit  = 20, 
                    load  = './data/rl/espn.com.p')

import time
for i in range(20):
    time.sleep(1)   
    rl.get('https://www.espn.com', waitForPop = True)
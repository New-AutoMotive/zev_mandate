import random
import requests
from bs4 import BeautifulSoup as bs

class rotatingIP:

    def __init__(self):

        self.ip_list_website = 'https://free-proxy-list.net/'
        self.check_website = 'https://ipinfo.io/json'
        return 

    def get_proxy(self):
        while(True):

            r = requests.get(self.ip_list_website)
            soup = bs(r.content, 'html.parser')
            table = soup.find('tbody')
            proxies = []
            for row in table:
                if row.find_all('td')[4].text =='elite proxy':
                    proxy = ':'.join([row.find_all('td')[0].text, row.find_all('td')[1].text])
                    proxies.append(proxy)
                else:
                    pass
            proxy = random.choice(proxies)
            return proxy

                
    def check_proxy(self,proxy):
        proxies = {'http':proxy}
        headers ={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:80.0) Gecko/20100101 Firefox/80.0'}
        
        session = requests.session()
        session.proxies.update(proxies)
        session.headers.update(headers)
        try:
            r = session.get(self.check_website,timeout=5 )
            return r.ok
        except:
            return False
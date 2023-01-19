import time

INTERVAL = 600

class Cache:
    def __init__(self, expire=600):
        self.cache = {}
        self.expire = expire
    
    def update(self, tag, res):
        self.cache[tag] = {
            'res': res,
            'expire': int(time.time()) + self.expire
        }

    def validate(self):
        self.cache = dict(filter(lambda x: x[1]['expire'] >= int(time.time()), self.cache.items()))
    
    def get(self, tag):
        if self.cache.get(tag):
            return self.cache.get(tag)['res']
        else:
            return None
#-*-coding:utf-8-*-

import json
import urllib2
import mylog
import redis

class BISyncService:
    def handle(self,conn):
        r = redis.Redis(host='10.4.6.39',port=6379,db=0)

        req = urllib2.Request('http://bi.jtjr99.com/export/appstat')
        response = urllib2.urlopen(req)
        data = response.read()
        js = json.loads(data)

        total_customer = long(js['data']['allusers'])
        total_amount = long(js['data']['tx_amount_all'])

        r.set('total_customer',total_customer)
        r.set('total_amount',total_amount)
        
        msg = 'total_customer:%s,total_amount:%s' % (total_customer,total_amount)
        mylog.info(msg)

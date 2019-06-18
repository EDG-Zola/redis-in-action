#! encoding=utf-8
import os
import time
import unittest
import uuid

import redis

#--------------- Redis事务：定义用户信息和用户包裹 ----------------#
def list_item(conn, itemid, sellerid, price):
    '''卖家sellerid将包裹里面的商品放到市场上销售'''
    inventory = "inventory:%s" % sellerid # 卖家包裹
    item = "%s.%s" % (itemid, sellerid) # 有序集合market:中存放的商品格式：商品id.卖家id
    end = time.time() + 5
    # 创建事务流水线，来执行事务操作
    pipe = conn.pipeline()
    # 如果执行时间小于5秒仍然为成功，则重试
    while time.time() < end:
        try:
            # 监视用户包裹是否发生变化，如果执行EXEC命令之前发生了变化，则Redis拒绝执行事务
            pipe.watch(inventory)
            # 检查用户包裹是否仍然持有该商品
            if not pipe.sismember(inventory, itemid):
                pipe.unwatch()
                return None
            # 准备执行事务，把一系列命令加入到队列中
            pipe.multi()
            # 将商品(有序结合的成员)和价格(有序集合的分值)加入到market:有序集合中
            pipe.zadd('market:', item, price)
            # 商品被加入到市场market:中，因此需要把商品移出卖家包裹
            pipe.srem(inventory, itemid)
            # 开始执行事务
            pipe.execute()
            # 事务执行成功，返回True
            return True
        # 如果在执行EXEC命令之前，有其他操作更改了监视的建，则引发WatchError
        except redis.exceptions.WatchError:
            pass
        # 5秒还未能执行事务，则返回False
        return False

#--------------- Redis事务：买家从市场购买商品 ----------------#
def purchase_item(conn, buyerid, itemid, sellerid, lprice):
    '''买家buyerid购买卖家sellerid放在市场上的商品itemid'''
    buyer = 'users:%s' % buyerid 
    seller = 'users:%s' % sellerid
    inventory = "inventory:%s" % buyerid # 买家包裹
    item = "%s.%s" % (itemid, sellerid) # 有序集合market中存放的商品格式：商品id.卖家id

    end = time.time() + 10
    pipe = conn.pipeline()

    while time.time() < end:
        try:
            # 监视市场和买家的个人信息进行监视
            pipe.watch('maret:', buyer)
            # 检查买家要购买的商品价格是否发生了变化或者买家是否有足够的钱来购买商品
            # 如果上述条件有一个满足，则取消监视
            price = pipe.zscore('market:', item)
            funds = int(pipe.hget(buyer, "funds"))
            if price != lprice or funds < price:
                pipe.unwatch()
                return None
            
            pipe.multi()
            # 卖家的钱包增加一个商品的价格，买家的钱包减少一个商品的价格
            pipe.hincrby(seller, "funds", int(price))
            pipe.hincrby(buyer, "funds", int(-price))
            # 买家的包裹增加一个商品
            pipe.sadd(inventory, itemid)
            # 市场上该商品被移除
            pipe.zrem('market:', item)
            pipe.execute()
            return True
        except redis.exceptions.WatchError:
            pass
        # 10秒还未能执行事务，则返回False
        return False


#---------------非事务非流水线 ----------------#
def update_token(conn, token, user, item=None):
    '''记录用户最近浏览的商品以及最近访问的页面'''
    timestamp = time.time()                             
    conn.hset('login:', token, user)                    
    conn.zadd('recent:', token, timestamp)              
    if item:
        conn.zadd('viewed:' + token, item, timestamp)   
        conn.zremrangebyrank('viewed:' + token, 0, -26) 
        conn.zincrby('viewed:', item, -1)

#---------------非事务流水线：进一步提高性能 ----------------#
def update_token_pipeline(conn, token, user, item=None):
    '''记录用户最近浏览的商品以及最近访问的页面'''
    timestamp = time.time()
    pipe = conn.pipeline()                             
    pipe.hset('login:', token, user)                    
    pipe.zadd('recent:', token, timestamp)              
    if item:
        pipe.zadd('viewed:' + token, item, timestamp)   
        pipe.zremrangebyrank('viewed:' + token, 0, -26) 
        pipe.zincrby('viewed:', item, -1)
    pipe.execute() 


def benchmark_update_token(conn, duration):
    '''测试一段时间duration之内，update_token和update_token_pipeline的性能'''
    for function in (update_token, update_token_pipeline):      
        count = 0                                               
        start = time.time()                                     
        end = start + duration                                  
        while time.time() < end:
            count += 1
            function(conn, 'token', 'user', 'item')             
        delta = time.time() - start                             
        print function.__name__, count, delta, count / delta    

#--------------- Below this line are helpers to test the code ----------------

class TestCh04(unittest.TestCase):
    def setUp(self):
        import redis
        self.conn = redis.Redis(db=15)
        self.conn.flushdb()

    def tearDown(self):
        self.conn.flushdb()
        del self.conn
        print
        print

    # We can't test process_logs, as that would require writing to disk, which
    # we don't want to do.

    # We also can't test wait_for_sync, as we can't guarantee that there are
    # multiple Redis servers running with the proper configuration

    def test_list_item(self):
        import pprint
        conn = self.conn

        print "We need to set up just enough state so that a user can list an item"
        seller = 'userX'
        item = 'itemX'
        conn.sadd('inventory:' + seller, item)
        i = conn.smembers('inventory:' + seller)
        print "The user's inventory has:", i
        self.assertTrue(i)
        print

        print "Listing the item..."
        l = list_item(conn, item, seller, 10)
        print "Listing the item succeeded?", l
        self.assertTrue(l)
        r = conn.zrange('market:', 0, -1, withscores=True)
        print "The market contains:"
        pprint.pprint(r)
        self.assertTrue(r)
        self.assertTrue(any(x[0] == 'itemX.userX' for x in r))

    def test_purchase_item(self):
        self.test_list_item()
        conn = self.conn
        
        print "We need to set up just enough state so a user can buy an item"
        buyer = 'userY'
        conn.hset('users:userY', 'funds', 125)
        r = conn.hgetall('users:userY')
        print "The user has some money:", r
        self.assertTrue(r)
        self.assertTrue(r.get('funds'))
        print

        print "Let's purchase an item"
        p = purchase_item(conn, 'userY', 'itemX', 'userX', 10)
        print "Purchasing an item succeeded?", p
        self.assertTrue(p)
        r = conn.hgetall('users:userY')
        print "Their money is now:", r
        self.assertTrue(r)
        i = conn.smembers('inventory:' + buyer)
        print "Their inventory is now:", i
        self.assertTrue(i)
        self.assertTrue('itemX' in i)
        self.assertEquals(conn.zscore('market:', 'itemX.userX'), None)

    def test_benchmark_update_token(self):
        benchmark_update_token(self.conn, 5)

if __name__ == '__main__':
    unittest.main()

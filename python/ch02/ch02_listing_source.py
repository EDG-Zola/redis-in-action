#!encoding=utf-8
import json
import threading
import time
import unittest
import urllib
import uuid

#--------------- 登陆和cookie缓存 ----------------#
def check_token(conn, token):
    '''从记录当前登陆用户及其令牌的哈希中获取给定令牌对应的用户'''
    return conn.hget('login:', token)

def update_token(conn, token, user, item=None):
    '''
    当用户登陆时，对登陆用户散列表进行更新；
    同时，将用户对应的令牌添加到记录最近登陆用户的有序集合中；
    最后，将此登陆用户浏览的商品添加到记录这个用户最近浏览过的商品有序集合中，
    当被记录的商品数量超过25个时，通过时间排序移除那些旧的浏览商品
    '''
    timestamp = time.time()
    conn.hset('login:', token, user)
    conn.zadd('recent:', token, timestamp)
    if item:
        conn.zadd('viewed:' + token, item, timestamp)
        # 通过时间戳进行排序，那么时间戳分值越大，则越靠后，
        # 这里删除从0到倒数第26名的浏览商品，剩下的及时最新浏览的25个商品
        conn.zremrangebyrank('viewed:' + token, 0, -26)
        # 记录所有商品的浏览次数
        conn.zincrby('viewed:', item, -1)

QUIT = False
LIMIT = 10000000 # 只保存1千万个登陆会话（即1千万个当前登陆用户与其令牌映射值）
def clean_sessions(conn):
    '''
    当记录最近登陆用户的有序集合超过1千万时，就从记录最近登陆用户的有序集合中移除100个最旧的令牌
    然后，从记录用户登陆信息的哈希表中移除这100个用户的信息，
    最后，从记录这些用户浏览商品的有序集合中清除这些键值对
    '''
    while not QUIT:
        # 获取目前已有的令牌数量
        size = conn.zcard('recent:')
        # 如果令牌数量未超过限制，则休眠1s后重新检查
        if size <= LIMIT:
            time.sleep(1)
            continue
        # 如果超过限制，则获取需要移除的令牌ID(即时间戳值最小的那部分ID)
        end_index = min(size-LIMIT, 100)
        tokens = conn.zrange('recent:', 0, end_index - 1)
        # 为即将被删除的令牌构建键值
        session_keys = []
        for token in tokens:
            session_keys.append('viewed:' + str(token))
        # 删除那些被移除的用户浏览过的商品
        conn.delete(*session_keys)
        # 删除那些被移除用户的登陆信息，并从记录最近登陆用户的有序集合删除被移除的用户
        conn.hdel('login:', *tokens)
        conn.zrem('recent:', *tokens)


#--------------- 实现购物车 ----------------#
def add_to_cart(conn, session, item, count):
    '''
    如果购物车内商品的输入小于0时，从购物车移除指定商品；否则更新商品的数量
    '''
    if count <= 0:
        conn.hdel('cart:' + session, item)
    else:
        conn.hset('cart' + session, item, count)

def clean_full_sessions(conn):
    '''清除会话，并清除与会话对应的用户的购物车'''
    while not QUIT:
        size = conn.zcard('recent:')
        if size <= LIMIT:
            time.sleep(1)
            continue
        end_index = min(size - LIMIT, 100)
        sessions = conn.zrange('recent:', 0 , end_index-1)
        session_keys = []
        for sess in sessions:
            session_keys.append('viewed:' + str(sess))
            session_keys.append('cart:' + str(sess))

        conn.delete(*sessions)
        conn.hdel('login:', *session_keys)
        conn.hdel('recent:', *session_keys)

#--------------- 实现网页缓存：对能够缓存的请求，将请求缓存到redis中，然后从redis中返回被缓存的页面 ----------------#
def cache_request(conn, request, callback):
    '''对请求进行缓存'''
    # 如果是对于不能缓存的请求，直接调用回调函数
    if not can_cache(conn, request):
        return callback(request)
    # 否则尝试从redis中查找被缓存的页面
    page_key = 'cache:' + hash_request(request)
    content = conn.get(page_key)
    # 如果页面没有被缓存，将调用回调函数生成的页面缓存到redis中，设置过期时间为5分钟
    if not content:
        content = callback(request)
        conn.setex(page_key, content, 300)
    # 否则直接从redis中找到的页面
    return content

#--------------- 实现数据行：通过缓存页面载入时所需的数据库行来减少页面所需的时间 ----------------#
def schedule_row_cache(conn, row_id, delay):
    '''对数据行进行调度'''
    # 设置数据行的延迟值
    conn.zadd('delay:', row_id, delay)
    # 设置对需要缓存的数据行进行调度的时间为当前时间
    conn.zadd('schedule:', row_id, time.time())

def cache_rows(conn):
    '''守护进程函数'''
    while not QUIT:
        # 获取下一个需要被缓存的数据行以及该行的调度时间戳，命令会返回一个包含0个或者1个元组的列表
        next = conn.zrange('schedule:', 0, 0, withscores=True)
        now = time.time()
        # 如果暂时没有数据行被缓存，或者被缓存的数据行的调度时间戳未到，则等待50ms后继续检查
        if not next or next[0][1] > now:
            time.sleep(.05)
            continue
        row_id = next[0][0]
        
        # 提前获取下一次调度的延迟时间,如果数据行的延迟时间小于0，从延迟有序集合和调度有序集合移除这个数据行
        delay = conn.zscore('delay:', row_id)
        if delay <= 0:
            conn.zrem('delay:', row_id)
            conn.zrem('schedule:', row_id)
            conn.delete('inv:' + row_id)
            continue
        # 如果延迟值大于0，缓存函数从数据库中取出这些行，
        # 将它们编码为JSON格式并存储到redis中，然后更新这些行的调度时间
        row = Inventory.get(row_id)
        conn.zadd('schedule:', row_id, now + delay)
        conn.set('inv:' + str(row_id), json.dumps(row.to_dict()))


#--------------- 对网页进行分析：实现对浏览次数多的商品排名 ----------------#
def rescale_viewed(conn):
    '''
    守护进程函数：对商品浏览次数进行更新
    '''
    while not QUIT:
        # 删除排名在20000名后的所有商品
        conn.zremrangebyrank('viewed:', 0, -20001)
        # 将浏览次数降低为原来的一般
        conn.zinterstore('viewed:', {'viewed:' : .5})
        time.sleep(5)

def can_cache(conn, request):
    # 从页面取出商品id
    item_id = extract_item_id(request)
    # 检查这个页面能否被缓存以及这个页面是否为商品
    if not item_id or is_dynamic(request):
        return False
    # 取得商品的浏览次数排名
    # 根据商品的浏览次数排名判断是否需要缓存这个页面 
    rank = conn.zrank('viewed:', item_id)
    return rank is not None and rank < 10000

#--------------- Below this line are helpers to test the code ----------------

def extract_item_id(request):
    parsed = urllib.urlparse(request)
    query = urllib.parse_qs(parsed.query)
    return (query.get('item') or [None])[0]

def is_dynamic(request):
    parsed = urllib.urlparse(request)
    query = urllib.parse_qs(parsed.query)
    return '_' in query

def hash_request(request):
    return str(hash(request))

class Inventory(object):
    def __init__(self, id):
        self.id = id

    @classmethod
    def get(cls, id):
        return Inventory(id)

    def to_dict(self):
        return {'id':self.id, 'data':'data to cache...', 'cached':time.time()}

class TestCh02(unittest.TestCase):
    def setUp(self):
        import redis
        self.conn = redis.Redis(host='127.0.0.1', port=6379)

    def tearDown(self):
        conn = self.conn
        to_del = (
            conn.keys('login:*') + conn.keys('recent:*') + conn.keys('viewed:*') +
            conn.keys('cart:*') + conn.keys('cache:*') + conn.keys('delay:*') + 
            conn.keys('schedule:*') + conn.keys('inv:*'))
        if to_del:
            self.conn.delete(*to_del)
        del self.conn
        global QUIT, LIMIT
        QUIT = False
        LIMIT = 10000000
        print
        print

    def test_login_cookies(self):
        conn = self.conn
        global LIMIT, QUIT
        token = str(uuid.uuid4())

        update_token(conn, token, 'username', 'itemX')
        print("We just logged-in/updated token:", token)
        print("For user:", 'username')
        print

        print("What username do we get when we look-up that token?")
        r = check_token(conn, token)
        print(r)
        print
        self.assertTrue(r)


        print("Let's drop the maximum number of cookies to 0 to clean them out")
        print("We will start a thread to do the cleaning, while we stop it later")

        LIMIT = 0
        t = threading.Thread(target=clean_sessions, args=(conn,))
        t.setDaemon(1) # to make sure it dies if we ctrl+C quit
        t.start()
        time.sleep(1)
        QUIT = True
        time.sleep(2)
        if t.isAlive():
            raise Exception("The clean sessions thread is still alive?!?")

        s = conn.hlen('login:')
        print("The current number of sessions still available is:", s)
        self.assertFalse(s)
        
    def test_shopping_cart_cookies(self):
        conn = self.conn
        global LIMIT, QUIT
        token = str(uuid.uuid4())

        print("We'll refresh our session...")
        update_token(conn, token, 'username', 'itemX')
        print("And add an item to the shopping cart")
        add_to_cart(conn, token, "itemY", 3)
        r = conn.hgetall('cart:' + token)
        print("Our shopping cart currently has:", r)
        print

        self.assertTrue(len(r) >= 1)

        print("Let's clean out our sessions and carts")
        LIMIT = 0
        t = threading.Thread(target=clean_full_sessions, args=(conn,))
        t.setDaemon(1) # to make sure it dies if we ctrl+C quit
        t.start()
        time.sleep(1)
        QUIT = True
        time.sleep(2)
        if t.isAlive():
            raise Exception("The clean sessions thread is still alive?!?")

        r = conn.hgetall('cart:' + token)
        print("Our shopping cart now contains:", r)

        self.assertFalse(r)

    def test_cache_request(self):
        conn = self.conn
        token = str(uuid.uuid4())

        def callback(request):
            return "content for " + request

        update_token(conn, token, 'username', 'itemX')
        url = 'http://test.com/?item=itemX'
        print("We are going to cache a simple request against", url)
        result = cache_request(conn, url, callback)
        print("We got initial content:", repr(result))
        print

        self.assertTrue(result)

        print("To test that we've cached the request, we'll pass a bad callback")
        result2 = cache_request(conn, url, None)
        print("We ended up getting the same response!", repr(result2))

        self.assertEquals(result, result2)

        self.assertFalse(can_cache(conn, 'http://test.com/'))
        self.assertFalse(can_cache(conn, 'http://test.com/?item=itemX&_=1234536'))

    def test_cache_rows(self):
        import pprint
        conn = self.conn
        global QUIT
        
        print("First, let's schedule caching of itemX every 5 seconds")
        schedule_row_cache(conn, 'itemX', 5)
        print("Our schedule looks like:")
        s = conn.zrange('schedule:', 0, -1, withscores=True)
        pprint.pprint(s)
        self.assertTrue(s)

        print("We'll start a caching thread that will cache the data...")
        t = threading.Thread(target=cache_rows, args=(conn,))
        t.setDaemon(1)
        t.start()

        time.sleep(1)
        print("Our cached data looks like:")
        r = conn.get('inv:itemX')
        print(repr(r))
        self.assertTrue(r)
        print
        print("We'll check again in 5 seconds...")
        time.sleep(5)
        print("Notice that the data has changed...")
        r2 = conn.get('inv:itemX')
        print(repr(r2))
        print
        self.assertTrue(r2)
        self.assertTrue(r != r2)

        print("Let's force un-caching")
        schedule_row_cache(conn, 'itemX', -1)
        time.sleep(1)
        r = conn.get('inv:itemX')
        print("The cache was cleared?", not r)
        print
        self.assertFalse(r)

        QUIT = True
        time.sleep(2)
        if t.isAlive():
            raise Exception("The database caching thread is still alive?!?")


if __name__ == '__main__':
    unittest.main()
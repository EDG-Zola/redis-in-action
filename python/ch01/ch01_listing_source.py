#!encoding=utf-8
import time
import unittest

ONE_WEEK_IN_SECONDS = 7 * 86400                    
VOTE_SCORE = 432                                   

def article_vote(conn, user, article):
    cutoff = time.time() - ONE_WEEK_IN_SECONDS      
    if conn.zscore('time:', article) < cutoff:      
        return

    article_id = article.partition(':')[-1]        
    if conn.sadd('voted:' + article_id, user):      
        conn.zincrby('score:', article, VOTE_SCORE) 
        conn.hincrby(article, 'votes', 1)           


def post_article(conn, user, title, link):
    '''用户user发布一篇文章，文章标题为title,链接为link'''
    # 生成一篇文章id号，如果键article:不存在，则生成的id号为0,；否则在原来的值上加1
    article_id = str(conn.incr('article:'))
    # 将发布文章的用户添加到记录文章已投票用户的集合里面
    voted = 'voted:' + article_id
    conn.sadd(voted, user)
    # 将记录文章已投票用户的集合键设置过期时间为一周，当一周过去后，将不能对该文章进行投票
    conn.expire(voted, ONE_WEEK_IN_SECONDS)
    
    # 将文章信息添加到记录文章信息的hash中
    now = time.time()
    article = 'article:' + article_id
    conn.hmset(article, {
        'title': title,
        'link': link,
        'poster': user,
        'time': now,
        'votes': 1,
    })
    # 将发布文章的评分加入到记录文章评分的有序集合中
    conn.zadd('score:', article, now + VOTE_SCORE)
    # 将发布文章的时间加入到记录文章时间的有序集合中    
    conn.zadd('time:', article, now)

    return article_id


ARTICLE_PER_PAGE = 25
def get_articles(conn, page, order='score:'):
    '''根据页面page，获取评分最高或最新发布的文章'''
    # 按评分从高到低或发布时间从新到旧获取文章id，范围为start——end
    start = (page - 1) * ARTICLE_PER_PAGE
    end = start + ARTICLE_PER_PAGE - 1
    ids = conn.zrevrange(order, start, end)
    # 获取每一篇文章的详细信息，存储在一个列表中
    articles = []
    for id in ids:
        article_data = conn.hgetall(id)
        article_data['id'] = id
        articles.append(article_data)
    return articles


def add_remove_groups(conn, article_id, to_add=[], to_remove=[]):
    '''将给定的文章添加到指定分组中(to_add)，或将给定文章移除指定分组(to_remove)'''
    article = 'article:' + article_id
    for group in to_add:
        conn.sadd('group:' + group, article)
    for group in to_remove:
        conn.srem('group:' + group, article)



def get_group_articles(conn, group, page, order='scoere:'):
    '''根据存储群组文章的集合和存储文章评分的有序集合，得到按文章评分排序的群组文章，同理，也可以得到按文章发布时间排序的群组文章'''
    key = order + group
    # 如果按文章评分排序的群组文章集合键不存在，则根据存储群组文章的集合和存储文章评分的有序集合,执行ZINTERSTORE命令创建该键，并设置过期时间为60s
    if not conn.exists(key):                    
        conn.zinterstore(key, 
            ['group:' + group, order],
            aggregate='max',
        )
        conn.expire(key, 60)
    return get_articles(conn, page, key)

#-------------- Below this line are helpers to test the code ----------------#

class TestCh01(unittest.TestCase):
    '''测试类'''
    def setUp(self):
        '''测试函数前的准备操作'''
        import redis
        self.conn = redis.Redis(db=15)

    def tearDown(self):
        '''测试函数后的销毁操作'''
        del self.conn
        print
        print

    def test_article_functionality(self):
        conn = self.conn
        import pprint

        article_id = str(post_article(conn, 'username', 'A title', 'http://www.google.com'))
        print("We posted a new article with id:", article_id)
        print
        self.assertTrue(article_id)

        print("Its HASH looks like:")
        r = conn.hgetall('article:' + article_id)
        print(r)
        print
        self.assertTrue(r)

        article_vote(conn, 'other_user', 'article:' + article_id)
        print("We voted for the article, it now has votes:",)
        v = int(conn.hget('article:' + article_id, 'votes'))
        print(v)
        print
        self.assertTrue(v > 1)

        print("The currently highest-scoring articles are:")
        articles = get_articles(conn, 1)
        pprint.pprint(articles)
        print

        self.assertTrue(len(articles) >= 1)

        add_remove_groups(conn, article_id, ['new-group'])
        print("We added the article to a new group, other articles include:")
        articles = get_group_articles(conn, 'new-group', 1)
        pprint.pprint(articles)
        print
        self.assertTrue(len(articles) >= 1)

        to_del = (
            conn.keys('time:*') + conn.keys('voted:*') + conn.keys('score:*') + 
            conn.keys('article:*') + conn.keys('group:*')
        )
        if to_del:
            conn.delete(*to_del)

if __name__ == '__main__':
    unittest.main()

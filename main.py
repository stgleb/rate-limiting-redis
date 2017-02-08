import json

import redis
import time


over_limit_multi_lua_ = '''
local limits = cjson.decode(ARGV[1])
local now = tonumber(ARGV[2])
for i, limit in ipairs(limits) do
    local duration = limit[1]

    local bucket = ':' .. duration .. ':' .. math.floor(now / duration)
    for j, id in ipairs(KEYS) do
        local key = id .. bucket

        local count = redis.call('INCR', key)
        redis.call('EXPIRE', key, duration)
        if tonumber(count) > limit[2] then
            return 1
        end
    end
end
return 0
'''


def over_limit(conn, resource, duration=3600, limit=240):
    bucket = ':{0}:{1}'.format(duration, time.time() // duration)

    key = resource + bucket
    count = conn.incr(key)
    conn.expire(key, duration)

    if count > limit:
        return True

    return False


def over_limit_multi(conn, resource, limits):
    for duration, limit in limits:
        if over_limit(conn, resource, duration, limit):
            return True

    return False


def over_limit_multi_lua(conn, resource, limits):
    if not hasattr(conn, 'over_limit_multi_lua'):
        conn.over_limit_multi_lua = conn.register_script(over_limit_multi_lua_)

    return conn.over_limit_multi_lua(
        keys=resource, args=[json.dumps(limits), time.time()])


if __name__ == '__main__':
    pool = redis.ConnectionPool(host='localhost', port=6379, db=0)
    conn = redis.Redis(connection_pool=pool)
    counter = 10
    # 100 per hour, but not  more that 1 per second
    limits = [(3600, 100), (1, 1)]

    while counter > 0:
        if not over_limit_multi(conn, 'resource', limits):
            counter -= 1
            print "Ok"
        else:
            time.sleep(1)

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


def _script_load(script):
    """
    Borrowed from my book, Redis in Action:
    https://github.com/josiahcarlson/redis-in-action/blob/master/python/ch11_listing_source.py

    Used because the API for the Python Lua scripting support is awkward.
    """
    sha = [None]

    def call(conn, keys=[], args=[], force_eval=False):
        if not force_eval:
            if not sha[0]:
                sha[0] = conn.execute_command(
                    "SCRIPT", "LOAD", script, parse="LOAD")
            try:
                return conn.execute_command(
                    "EVALSHA", sha[0], len(keys), *(keys+args))
            except redis.exceptions.ResponseError as msg:
                if not msg.args[0].startswith("NOSCRIPT"):
                    raise
        return conn.execute_command(
            "EVAL", script, len(keys), *(keys+args))
    return call


def over_limit_multi_lua(conn, resource, limits):
    if not hasattr(conn, 'over_limit_multi_lua'):
        conn.over_limit_multi_lua = conn.register_script(over_limit_multi_lua_)

    result = conn.over_limit_multi_lua(
        keys=resource, args=[json.dumps(limits), time.time()])

    return result


over_limit_lua = _script_load('''
local slice = {1, 60, 3600, 86400}
local dkeys = {'s', 'm', 'h', 'd'}
local ts = tonumber(table.remove(ARGV))
local weight = tonumber(table.remove(ARGV))
local fail = false

-- only update the counts if all of the limits are okay
for _, ready in ipairs({false, true}) do
    for i = 1, math.min(#ARGV, #slice) do
        local limit = tonumber(ARGV[i])

        -- only check limits that are worthwhile
        if limit > 0 then
            local suff = ':' .. dkeys[i] .. ':' .. math.floor(ts / slice[i])
            local remain = 1 + slice[i] - math.fmod(ts, slice[i])
            for j, k in ipairs(KEYS) do
                local key = k .. suff
                if ready then
                    redis.call('incrby', key, weight)
                    redis.call('expire', key, remain)
                else
                    local total = tonumber(redis.call('get', key) or '0')
                    if total + weight > limit then
                        fail = true
                        break
                    end
                end
            end
        end
    end
    if fail then
        break
    end
end

return fail
''')


def over_limit2(conn, base_keys, second=0, minute=0, hour=0, day=0, weight=1):
    """
    :param conn: a Redis connection object
    :param base_keys: List of user identifiers
    :param second: limit per second
    :param minute: limit per minute
    :param hour: limit per hour
    :param day: limit per day
    :param weight:
    :return:
    """
    limits = [second, minute, hour, day, weight, int(time.time())]
    return bool(over_limit_lua(conn, keys=base_keys, args=limits))


if __name__ == '__main__':
    pool = redis.ConnectionPool(host='localhost', port=6379, db=0)
    conn = redis.Redis(connection_pool=pool)
    counter = 10
    # 100 per hour, but not  more that 1 per second
    limits = [(3600, 100), (2, 2)]

    while counter > 0:
        if not over_limit2(conn, ["resource"], second=1, minute=5):
            counter -= 1
            print "Ok"
        else:
            time.sleep(1)

from redis.sentinel import Sentinel

# detect if a connection is a sentinel
def Is_Sentinel(conn):
    info = conn.info(section="server")
    return "redis_mode" in info and info["redis_mode"] == "sentinel"

# create a sentinel connection from a Redis connection
def Sentinel_Conn(conn, ssl):
    # collect masters
    masters = conn.sentinel_masters()

    # abort if multiple masters are detected
    if len(masters) != 1:
        raise Exception("Multiple masters, require service name")

    # monitored service name
    service_name = list(masters.keys())[0]

    # list of sentinels connection information
    sentinels_conns = []

    # current sentinel
    host = conn.connection_pool.connection_kwargs['host']
    port = conn.connection_pool.connection_kwargs['port']
    sentinels_conns.append((host, port))

    # additional sentinels
    #sentinels = conn.sentinel_sentinels(service_name)
    #for sentinel in sentinels:
    #    ip = sentinel['ip']
    #    port = sentinel['port']
    #    sentinels_conns.append((host, port))

    # use the same connection arguments e.g. username and password
    connection_kwargs = conn.connection_pool.connection_kwargs

    # construct sentinel arguments
    sentinel_kwargs = { }
    if 'username' in connection_kwargs:
        sentinel_kwargs['username'] = connection_kwargs['username']
    if 'password' in connection_kwargs:
        sentinel_kwargs['password'] = connection_kwargs['password']
    if ssl:
        sentinel_kwargs['ssl'] = True

    return (Sentinel(sentinels_conns, sentinel_kwargs=sentinel_kwargs, **connection_kwargs), service_name)


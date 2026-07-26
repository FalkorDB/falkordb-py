from redis.sentinel import Sentinel  # type: ignore[import-not-found]


# detect if a connection is a sentinel
def Is_Sentinel(conn):
    info = conn.info(section="server")
    return "redis_mode" in info and info["redis_mode"] == "sentinel"


# create a sentinel connection from a Redis connection
def Sentinel_Conn(conn, ssl, service_name=None, sentinel_nodes=None):
    # use the same connection arguments e.g. username and password
    connection_kwargs = conn.connection_pool.connection_kwargs.copy()

    # list of sentinels connection information
    sentinels_conns = list(sentinel_nodes or [])
    if len(sentinels_conns) == 0:
        # current sentinel
        host = connection_kwargs.get("host")
        port = connection_kwargs.get("port")
        sentinels_conns.append((host, port))

    # additional sentinels
    # sentinels = conn.sentinel_sentinels(service_name)
    # for sentinel in sentinels:
    #    ip = sentinel['ip']
    #    port = sentinel['port']
    #    sentinels_conns.append((host, port))
    if service_name is None:
        # collect masters
        masters = conn.sentinel_masters()
        # abort if multiple masters are detected
        if len(masters) != 1:
            raise Exception("Multiple masters, require service name")
        # monitored service name
        service_name = list(masters.keys())[0]

    # construct sentinel arguments
    sentinel_kwargs = {}
    if "username" in connection_kwargs:
        sentinel_kwargs["username"] = connection_kwargs["username"]
    if "password" in connection_kwargs:
        sentinel_kwargs["password"] = connection_kwargs["password"]
    if ssl:
        sentinel_kwargs["ssl"] = True
    connection_kwargs.pop("host", None)
    connection_kwargs.pop("port", None)
    connection_kwargs.pop("connection_pool", None)

    return (
        Sentinel(sentinels_conns, sentinel_kwargs=sentinel_kwargs, **connection_kwargs),
        service_name,
    )

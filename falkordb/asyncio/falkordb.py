import redis.asyncio as redis
from .cluster import *
from .graph import AsyncGraph
from typing import List, Union

# config command
LIST_CMD   = "GRAPH.LIST"
CONFIG_CMD = "GRAPH.CONFIG"

class FalkorDB():
    """
    Asynchronous FalkorDB Class for interacting with a FalkorDB server.

    Usage example::
        from falkordb.asyncio import FalkorDB
        # connect to the database and select the 'social' graph
        db = FalkorDB()
        graph = db.select_graph("social")

        # get a single 'Person' node from the graph and print its name
        response = await graph.query("MATCH (n:Person) RETURN n LIMIT 1")
        result   = response.result_set
        person   = result[0][0]
        print(node.properties['name'])
    """

    def __init__(
            self,
            host='localhost',
            port=6379,
            password=None,
            socket_timeout=None,
            socket_connect_timeout=None,
            socket_keepalive=None,
            socket_keepalive_options=None,
            connection_pool=None,
            unix_socket_path=None,
            encoding='utf-8',
            encoding_errors='strict',
            retry_on_timeout=False,
            retry_on_error=None,
            ssl=False,
            ssl_keyfile=None,
            ssl_certfile=None,
            ssl_cert_reqs='required',
            ssl_ca_certs=None,
            ssl_ca_data=None,
            ssl_check_hostname=False,
            max_connections=None,
            single_connection_client=False,
            health_check_interval=0,
            client_name=None,
            lib_name='FalkorDB',
            lib_version='1.0.0',
            username=None,
            retry=None,
            connect_func=None,
            credential_provider=None,
            protocol=2,
            # FalkorDB Cluster Params
            cluster_error_retry_attempts=3,
            startup_nodes=None,
            require_full_coverage=False,
            reinitialize_steps=5,
            read_from_replicas=False,
            address_remap=None,
        ):

        conn = redis.Redis(host=host, port=port, db=0, password=password,
                           socket_timeout=socket_timeout,
                           socket_connect_timeout=socket_connect_timeout,
                           socket_keepalive=socket_keepalive,
                           socket_keepalive_options=socket_keepalive_options,
                           connection_pool=connection_pool,
                           unix_socket_path=unix_socket_path,
                           encoding=encoding, encoding_errors=encoding_errors,
                           decode_responses=True,
                           retry_on_timeout=retry_on_timeout,
                           retry_on_error=retry_on_error, ssl=ssl,
                           ssl_keyfile=ssl_keyfile, ssl_certfile=ssl_certfile,
                           ssl_cert_reqs=ssl_cert_reqs,
                           ssl_ca_certs=ssl_ca_certs,
                           ssl_ca_data=ssl_ca_data,
                           ssl_check_hostname=ssl_check_hostname,
                           max_connections=max_connections,
                           single_connection_client=single_connection_client,
                           health_check_interval=health_check_interval,
                           client_name=client_name, lib_name=lib_name,
                           lib_version=lib_version, username=username,
                           retry=retry, redis_connect_func=connect_func,
                           credential_provider=credential_provider,
                           protocol=protocol)
        
        if Is_Cluster(conn):
            conn = Cluster_Conn(
                conn,
                ssl,
                cluster_error_retry_attempts,
                startup_nodes,
                require_full_coverage,
                reinitialize_steps,
                read_from_replicas,
                address_remap,
            )

        self.connection      = conn
        self.flushdb         = conn.flushdb
        self.execute_command = conn.execute_command

    @classmethod
    def from_url(cls, url: str, **kwargs) -> "FalkorDB":
        """
        Creates a new FalkorDB instance from a URL.

        Args:
            cls: The class itself.
            url (str): The URL.
            kwargs: Additional keyword arguments to pass to the ``DB.from_url`` function.

        Returns:
            DB: A new DB instance.

        Usage example::
        db = FalkorDB.from_url("falkor://[[username]:[password]]@localhost:6379")
        db = FalkorDB.from_url("falkors://[[username]:[password]]@localhost:6379")
        db = FalkorDB.from_url("unix://[username@]/path/to/socket.sock?db=0[&password=password]")
        """

        db = cls()

        # switch from redis:// to falkordb://
        if url.startswith('falkor://'):
            url = 'redis://' + url[len('falkor://'):]
        elif url.startswith('falkors://'):
            url = 'rediss://' + url[len('falkors://'):]

        conn = redis.from_url(url, **kwargs)
        db.connection      = conn
        db.flushdb         = conn.flushdb
        db.execute_command = conn.execute_command

        return db

    def select_graph(self, graph_id: str) -> AsyncGraph:
        """
        Selects a graph by creating a new Graph instance.

        Args:
            graph_id (str): The identifier of the graph.

        Returns:
            AsyncGraph: A new Graph instance associated with the selected graph.
        """
        if not isinstance(graph_id, str) or graph_id == "":
            raise TypeError(f"Expected a string parameter, but received {type(graph_id)}.")

        return AsyncGraph(self, graph_id)

    async def list_graphs(self) -> List[str]:
        """
        Lists all graph names.
        See: https://docs.falkordb.com/commands/graph.list.html

        Returns:            
            List: List of graph names.

        """

        return await self.connection.execute_command(LIST_CMD)

    async def config_get(self, name: str) -> Union[int, str]:
        """
        Retrieve a DB level configuration.
        For a list of available configurations see: https://docs.falkordb.com/configuration.html#falkordb-configuration-parameters

        Args:
            name (str): The name of the configuration.

        Returns:
            int or str: The configuration value.

        """

        res = await self.connection.execute_command(CONFIG_CMD, "GET", name)
        return res[1]

    async def config_set(self, name: str, value=None) -> None:
        """
        Update a DB level configuration.
        For a list of available configurations see: https://docs.falkordb.com/configuration.html#falkordb-configuration-parameters

        Args:
            name (str): The name of the configuration.
            value: The value to set.

        Returns:
            None

        """

        return await self.connection.execute_command(CONFIG_CMD, "SET", name, value)

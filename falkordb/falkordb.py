import redis
from .cluster import *
from .sentinel import *
from .graph import Graph
from typing import List, Union

# config command
LIST_CMD = "GRAPH.LIST"
CONFIG_CMD = "GRAPH.CONFIG"


class FalkorDB:
    """
    FalkorDB Class for interacting with a FalkorDB server.

    Usage example::
        from falkordb import FalkorDB
        # connect to the database and select the 'social' graph
        db = FalkorDB()
        graph = db.select_graph("social")

        # get a single 'Person' node from the graph and print its name
        result = graph.query("MATCH (n:Person) RETURN n LIMIT 1").result_set
        person = result[0][0]
        print(node.properties['name'])
    """

    def __init__(
        self,
        host="localhost",
        port=6379,
        password=None,
        socket_timeout=None,
        socket_connect_timeout=None,
        socket_keepalive=None,
        socket_keepalive_options=None,
        connection_pool=None,
        unix_socket_path=None,
        encoding="utf-8",
        encoding_errors="strict",
        charset=None,
        errors=None,
        retry_on_timeout=False,
        retry_on_error=None,
        ssl=False,
        ssl_keyfile=None,
        ssl_certfile=None,
        ssl_cert_reqs="required",
        ssl_ca_certs=None,
        ssl_ca_path=None,
        ssl_ca_data=None,
        ssl_check_hostname=False,
        ssl_password=None,
        ssl_validate_ocsp=False,
        ssl_validate_ocsp_stapled=False,
        ssl_ocsp_context=None,
        ssl_ocsp_expected_cert=None,
        max_connections=None,
        single_connection_client=False,
        health_check_interval=0,
        client_name=None,
        lib_name="FalkorDB",
        lib_version="1.0.0",
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
        dynamic_startup_nodes=True,
        url=None,
        address_remap=None,
        decode_responses=True
    ):

        conn = redis.Redis(
            host=host,
            port=port,
            db=0,
            password=password,
            socket_timeout=socket_timeout,
            socket_connect_timeout=socket_connect_timeout,
            socket_keepalive=socket_keepalive,
            socket_keepalive_options=socket_keepalive_options,
            connection_pool=connection_pool,
            unix_socket_path=unix_socket_path,
            encoding=encoding,
            encoding_errors=encoding_errors,
            charset=charset,
            errors=errors,
            decode_responses=decode_responses,
            retry_on_timeout=retry_on_timeout,
            retry_on_error=retry_on_error,
            ssl=ssl,
            ssl_keyfile=ssl_keyfile,
            ssl_certfile=ssl_certfile,
            ssl_cert_reqs=ssl_cert_reqs,
            ssl_ca_certs=ssl_ca_certs,
            ssl_ca_path=ssl_ca_path,
            ssl_ca_data=ssl_ca_data,
            ssl_check_hostname=ssl_check_hostname,
            ssl_password=ssl_password,
            ssl_validate_ocsp=ssl_validate_ocsp,
            ssl_validate_ocsp_stapled=ssl_validate_ocsp_stapled,
            ssl_ocsp_context=ssl_ocsp_context,
            ssl_ocsp_expected_cert=ssl_ocsp_expected_cert,
            max_connections=max_connections,
            single_connection_client=single_connection_client,
            health_check_interval=health_check_interval,
            client_name=client_name,
            lib_name=lib_name,
            lib_version=lib_version,
            username=username,
            retry=retry,
            redis_connect_func=connect_func,
            credential_provider=credential_provider,
            protocol=protocol,
        )


        self.password=password
        self.socket_timeout=socket_timeout
        self.socket_connect_timeout=socket_connect_timeout
        self.socket_keepalive=socket_keepalive
        self.socket_keepalive_options=socket_keepalive_options
        self.connection_pool=connection_pool
        self.unix_socket_path=unix_socket_path
        self.encoding=encoding
        self.encoding_errors=encoding_errors
        self.charset=charset
        self.errors=errors
        self.retry_on_timeout=retry_on_timeout
        self.retry_on_error=retry_on_error
        self.ssl=ssl
        self.ssl_keyfile=ssl_keyfile
        self.ssl_certfile=ssl_certfile
        self.ssl_cert_reqs=ssl_cert_reqs
        self.ssl_ca_certs=ssl_ca_certs
        self.ssl_ca_path=ssl_ca_path
        self.ssl_ca_data=ssl_ca_data
        self.ssl_check_hostname=ssl_check_hostname
        self.ssl_password=ssl_password
        self.ssl_validate_ocsp=ssl_validate_ocsp
        self.ssl_validate_ocsp_stapled=ssl_validate_ocsp_stapled
        self.ssl_ocsp_context=ssl_ocsp_context
        self.ssl_ocsp_expected_cert=ssl_ocsp_expected_cert
        self.max_connections=max_connections
        self.single_connection_client=single_connection_client
        self.health_check_interval=health_check_interval
        self.client_name=client_name
        self.lib_name=lib_name
        self.lib_version=lib_version
        self.username=username
        self.retry=retry
        self.connect_func=connect_func
        self.credential_provider=credential_provider
        self.protocol=protocol
        # FalkorDB Cluster Params
        self.cluster_error_retry_attempts=cluster_error_retry_attempts
        self.startup_nodes=startup_nodes
        self.require_full_coverage=require_full_coverage
        self.reinitialize_steps=reinitialize_steps
        self.read_from_replicas=read_from_replicas
        self.dynamic_startup_nodes=dynamic_startup_nodes
        self.url=url
        self.address_remap=address_remap
        self.sentinel_flag = False
        self.cluster_flag = False
        if Is_Sentinel(conn):
            self.sentinel, self.service_name = Sentinel_Conn(conn, ssl)
            conn = self.sentinel.master_for(self.service_name, ssl=ssl)
            self.sentinel_flag = True

        if Is_Cluster(conn):
            conn = Cluster_Conn(
                conn,
                ssl,
                cluster_error_retry_attempts,
                startup_nodes,
                require_full_coverage,
                reinitialize_steps,
                read_from_replicas,
                dynamic_startup_nodes,
                url,
                address_remap,
            )
            self.cluster_flag = True

        self.connection = conn
        self.flushdb = conn.flushdb
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

        # switch from redis:// to falkordb://
        if url.startswith("falkor://"):
            url = "redis://" + url[len("falkor://") :]
        elif url.startswith("falkors://"):
            url = "rediss://" + url[len("falkors://") :]

        conn = redis.from_url(url, **kwargs)

        connection_kwargs = conn.connection_pool.connection_kwargs
        kwargs["host"] = connection_kwargs.get("host", "localhost")
        kwargs["port"] = connection_kwargs.get("port", 6379)
        kwargs["username"] = connection_kwargs.get("username")
        kwargs["password"] = connection_kwargs.get("password")

        # Initialize a FalkorDB instance using the updated kwargs
        db = cls(**kwargs)

        return db

    def select_graph(self, graph_id: str) -> Graph:
        """
        Selects a graph by creating a new Graph instance.

        Args:
            graph_id (str): The identifier of the graph.

        Returns:
            Graph: A new Graph instance associated with the selected graph.
        """
        if not isinstance(graph_id, str) or graph_id == "":
            raise TypeError(
                f"Expected a string parameter, but received {type(graph_id)}."
            )

        return Graph(self, graph_id)

    def list_graphs(self) -> List[str]:
        """
        Lists all graph names.
        See: https://docs.falkordb.com/commands/graph.list.html

        Returns:
            List: List of graph names.

        """

        return self.connection.execute_command(LIST_CMD)

    def config_get(self, name: str) -> Union[int, str]:
        """
        Retrieve a DB level configuration.
        For a list of available configurations see: https://docs.falkordb.com/configuration.html#falkordb-configuration-parameters

        Args:
            name (str): The name of the configuration.

        Returns:
            int or str: The configuration value.

        """

        return self.connection.execute_command(CONFIG_CMD, "GET", name)[1]


    def get_replica_connections(self):
        #decide if its Sentinel or cluster
        if self.sentinel_flag:
            replica_hostnames = self.sentinel.discover_slaves(service_name=self.service_name)
            result = [FalkorDB( host=host,
                                password=self.password,
                                socket_timeout=self.socket_timeout,
                                socket_connect_timeout=self.socket_connect_timeout,
                                socket_keepalive=self.socket_keepalive,
                                socket_keepalive_options=self.socket_keepalive_options,
                                connection_pool=self.connection_pool,
                                unix_socket_path=self.unix_socket_path,
                                encoding=self.encoding,
                                encoding_errors=self.encoding_errors,
                                charset=self.charset,
                                errors=self.errors,
                                retry_on_timeout=self.retry_on_timeout,
                                retry_on_error=self.retry_on_error,
                                ssl=self.ssl,
                                ssl_keyfile=self.ssl_keyfile,
                                ssl_certfile=self.ssl_certfile,
                                ssl_cert_reqs=self.ssl_cert_reqs,
                                ssl_ca_certs=self.ssl_ca_certs,
                                ssl_ca_path=self.ssl_ca_path,
                                ssl_ca_data=self.ssl_ca_data,
                                ssl_check_hostname=self.ssl_check_hostname,
                                ssl_password=self.ssl_password,
                                ssl_validate_ocsp=self.ssl_validate_ocsp,
                                ssl_validate_ocsp_stapled=self.ssl_validate_ocsp_stapled,
                                ssl_ocsp_context=self.ssl_ocsp_context,
                                ssl_ocsp_expected_cert=self.ssl_ocsp_expected_cert,
                                max_connections=self.max_connections,
                                single_connection_client=self.single_connection_client,
                                health_check_interval=self.health_check_interval,
                                client_name=self.client_name,
                                lib_name=self.lib_name,
                                lib_version=self.lib_version,
                                username=self.username,
                                retry=self.retry,
                                connect_func=self.connect_func,
                                credential_provider=self.credential_provider,
                                protocol=self.protocol,
                                # FalkorDB Cluster Params
                                cluster_error_retry_attempts=self.cluster_error_retry_attempts,
                                startup_nodes=self.startup_nodes,
                                require_full_coverage=self.require_full_coverage,
                                reinitialize_steps=self.reinitialize_steps,
                                read_from_replicas=self.read_from_replicas,
                                dynamic_startup_nodes=self.dynamic_startup_nodes,
                                url=self.url,
                                address_remap=self.address_remap,
                                port=port) 
                    for host, port in replica_hostnames]
            return result
        elif self.cluster_flag:
            data = self.connection.cluster_nodes()
            # List comprehension to get a list of (ip, port, hostname) tuples
            host_port_list = [(ip_port.split(':')[0], ip_port.split(':')[1], flag['hostname']) for ip_port, flag in data.items() if 'slave' in flag["flags"]]
            result = [FalkorDB(
                host=tup[2],
                password=self.password,
                socket_timeout=self.socket_timeout,
                socket_connect_timeout=self.socket_connect_timeout,
                socket_keepalive=self.socket_keepalive,
                socket_keepalive_options=self.socket_keepalive_options,
                connection_pool=self.connection_pool,
                unix_socket_path=self.unix_socket_path,
                encoding=self.encoding,
                encoding_errors=self.encoding_errors,
                charset=self.charset,
                errors=self.errors,
                retry_on_timeout=self.retry_on_timeout,
                retry_on_error=self.retry_on_error,
                ssl=self.ssl,
                ssl_keyfile=self.ssl_keyfile,
                ssl_certfile=self.ssl_certfile,
                ssl_cert_reqs=self.ssl_cert_reqs,
                ssl_ca_certs=self.ssl_ca_certs,
                ssl_ca_path=self.ssl_ca_path,
                ssl_ca_data=self.ssl_ca_data,
                ssl_check_hostname=self.ssl_check_hostname,
                ssl_password=self.ssl_password,
                ssl_validate_ocsp=self.ssl_validate_ocsp,
                ssl_validate_ocsp_stapled=self.ssl_validate_ocsp_stapled,
                ssl_ocsp_context=self.ssl_ocsp_context,
                ssl_ocsp_expected_cert=self.ssl_ocsp_expected_cert,
                max_connections=self.max_connections,
                single_connection_client=self.single_connection_client,
                health_check_interval=self.health_check_interval,
                client_name=self.client_name,
                lib_name=self.lib_name,
                lib_version=self.lib_version,
                username=self.username,
                retry=self.retry,
                connect_func=self.connect_func,
                credential_provider=self.credential_provider,
                protocol=self.protocol,
                # FalkorDB Cluster Params
                cluster_error_retry_attempts=self.cluster_error_retry_attempts,
                startup_nodes=self.startup_nodes,
                require_full_coverage=self.require_full_coverage,
                reinitialize_steps=self.reinitialize_steps,
                read_from_replicas=self.read_from_replicas,
                dynamic_startup_nodes=self.dynamic_startup_nodes,
                url=self.url,
                address_remap=self.address_remap,
                port=tup[1]
            )
                     for tup in host_port_list]
            return result
            

    def config_set(self, name: str, value=None) -> None:
        """
        Update a DB level configuration.
        For a list of available configurations see: https://docs.falkordb.com/configuration.html#falkordb-configuration-parameters

        Args:
            name (str): The name of the configuration.
            value: The value to set.

        Returns:
            None

        """

        return self.connection.execute_command(CONFIG_CMD, "SET", name, value)

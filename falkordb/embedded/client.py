# Copyright (c) 2015, Yahoo Inc.
# Copyright (c) 2024, FalkorDB (adapted)
# Licensed under the MIT License
"""
Embedded FalkorDB client that manages a local Redis+FalkorDB process.
"""
import atexit
import json
import logging
import os
import shutil
import subprocess
import tempfile
import time
import redis

from . import configuration


logger = logging.getLogger(__name__)


class EmbeddedFalkorDBException(Exception):
    """Embedded FalkorDB client error exception."""
    pass


class EmbeddedFalkorDBServerStartError(Exception):
    """Embedded FalkorDB redis-server start error."""
    pass


class EmbeddedFalkorDB:
    """
    Manages an embedded Redis+FalkorDB server process.
    
    This class handles starting, configuring, and stopping a local Redis server
    with the FalkorDB module loaded, and provides a Redis connection to it.
    """
    
    def __init__(
        self,
        dbfilename=None,
        serverconfig=None,
        redis_executable='redis-server',
        falkordb_module=None,
    ):
        """
        Initialize an embedded FalkorDB instance.
        
        Parameters:
            dbfilename (str): Path to the database file for persistence
            serverconfig (dict): Additional Redis server configuration
            redis_executable (str): Path to redis-server executable
            falkordb_module (str): Path to FalkorDB module (.so file)
        """
        self.redis_dir = None
        self.pidfile = None
        self.socket_file = None
        self.logfile = None
        self.running = False
        self.dbfilename = 'redis.db'
        self.dbdir = None
        self.settingregistryfile = None
        self.redis_configuration = None
        self.redis_configuration_filename = None
        self.server_config = serverconfig or {}
        self.start_timeout = 10
        self.redis_executable = redis_executable
        self.falkordb_module = falkordb_module
        self._connection = None
        
        # Process dbfilename
        if dbfilename and dbfilename == os.path.basename(dbfilename):
            dbfilename = os.path.join(os.getcwd(), dbfilename)
        
        if dbfilename:
            self.dbfilename = os.path.basename(dbfilename)
            self.dbdir = os.path.dirname(dbfilename)
            self.settingregistryfile = os.path.join(
                self.dbdir, self.dbfilename + '.settings'
            )
        
        logger.debug('Setting up redis with rdb file: %s', self.dbfilename)
        
        # Register cleanup on exit
        atexit.register(self._cleanup)
        
        # Check if redis is already running for this database
        if self._is_redis_running() and not self.socket_file:
            self._load_setting_registry()
            logger.debug('Socket file after registry load: %s', self.socket_file)
        else:
            self._create_redis_directory_tree()
            
            if not self.dbdir:
                self.dbdir = self.redis_dir
                self.settingregistryfile = os.path.join(
                    self.dbdir, self.dbfilename + '.settings'
                )
            
            self._start_redis()
        
        # Create Redis connection
        self._connection = redis.Redis(
            unix_socket_path=self.socket_file,
            decode_responses=True,
        )
        
        logger.debug("Pinging the server to ensure we're connected")
        self._wait_for_server_start()
    
    @property
    def connection(self):
        """Get the Redis connection."""
        return self._connection
    
    @property
    def pid(self):
        """Get the current redis-server process id."""
        if self.pidfile and os.path.exists(self.pidfile):
            with open(self.pidfile) as fh:
                pid = int(fh.read().strip())
                if pid:
                    import psutil
                    try:
                        process = psutil.Process(pid)
                        if process.is_running():
                            return pid
                    except psutil.NoSuchProcess:
                        pass
        return 0
    
    def _create_redis_directory_tree(self):
        """Create a temp directory for the redis instance."""
        if not self.redis_dir:
            self.redis_dir = tempfile.mkdtemp()
            logger.debug('Creating temporary redis directory %s', self.redis_dir)
            self.pidfile = os.path.join(self.redis_dir, 'redis.pid')
            self.logfile = os.path.join(self.redis_dir, 'redis.log')
            if not self.socket_file:
                self.socket_file = os.path.join(self.redis_dir, 'redis.socket')
    
    def _start_redis(self):
        """Start the redis server with FalkorDB module."""
        self.redis_configuration_filename = os.path.join(
            self.redis_dir, 'redis.config'
        )
        
        kwargs = dict(self.server_config)
        kwargs.update({
            'pidfile': self.pidfile,
            'logfile': kwargs.get('logfile', self.logfile),
            'unixsocket': self.socket_file,
            'dbdir': self.dbdir,
            'dbfilename': self.dbfilename
        })
        
        # Write redis.config
        self.redis_configuration = configuration.config(**kwargs)
        with open(self.redis_configuration_filename, 'w') as fh:
            fh.write(self.redis_configuration)
        
        # Build command
        command = [self.redis_executable, self.redis_configuration_filename]
        
        # Load FalkorDB module if available
        if self.falkordb_module and os.path.exists(self.falkordb_module):
            command.extend(['--loadmodule', self.falkordb_module])
            logger.debug('Loading FalkorDB module: %s', self.falkordb_module)
        else:
            raise EmbeddedFalkorDBException(
                'FalkorDB module not found. Please install the embedded extra: '
                'pip install falkordb[embedded]'
            )
        
        logger.debug('Running: %s', ' '.join(command))
        rc = subprocess.call(command)
        if rc:
            logger.debug('The binary redis-server failed to start')
            logger.debug('Redis Server log:\n%s', self._redis_log)
            raise EmbeddedFalkorDBException('The binary redis-server failed to start')
        
        # Wait for Redis to start
        timeout = True
        for i in range(0, self.start_timeout * 10):
            if os.path.exists(self.socket_file):
                timeout = False
                break
            time.sleep(.1)
        
        if timeout:
            logger.debug('Redis Server log:\n%s', self._redis_log)
            raise EmbeddedFalkorDBServerStartError(
                'The redis-server process failed to start'
            )
        
        if not os.path.exists(self.socket_file):
            logger.debug('Redis Server log:\n%s', self._redis_log)
            raise EmbeddedFalkorDBException(
                f'Redis socket file {self.socket_file} is not present'
            )
        
        self._save_setting_registry()
        self.running = True
    
    def _wait_for_server_start(self):
        """Wait until the server is ready to receive requests."""
        timeout = True
        for i in range(0, self.start_timeout * 10):
            try:
                self._connection.ping()
                timeout = False
                break
            except redis.BusyLoadingError:
                pass
            except Exception:
                pass
            time.sleep(.1)
        
        if timeout:
            raise EmbeddedFalkorDBServerStartError(
                f'The redis-server process failed to start; unreachable after '
                f'{self.start_timeout} seconds'
            )
    
    def _is_redis_running(self):
        """Determine if there is a config setting for a currently running redis."""
        if not self.settingregistryfile:
            return False
        
        if os.path.exists(self.settingregistryfile):
            with open(self.settingregistryfile) as fh:
                settings = json.load(fh)
            
            if not os.path.exists(settings['pidfile']):
                return False
            
            with open(settings['pidfile']) as fh:
                pid = int(fh.read().strip())
                if pid:
                    import psutil
                    try:
                        process = psutil.Process(pid)
                        if not process.is_running():
                            return False
                    except psutil.NoSuchProcess:
                        return False
                else:
                    return False
            return True
        return False
    
    def _save_setting_registry(self):
        """Save the current settings to the registry file."""
        if self.settingregistryfile:
            settings = {
                'pidfile': self.pidfile,
                'unixsocket': self.socket_file,
            }
            with open(self.settingregistryfile, 'w') as fh:
                json.dump(settings, fh, indent=4)
    
    def _load_setting_registry(self):
        """Load settings from the registry file."""
        if self.settingregistryfile and os.path.exists(self.settingregistryfile):
            with open(self.settingregistryfile) as fh:
                settings = json.load(fh)
            self.pidfile = settings.get('pidfile')
            self.socket_file = settings.get('unixsocket')
    
    @property
    def _redis_log(self):
        """Get Redis server log content."""
        if self.logfile and os.path.exists(self.logfile):
            with open(self.logfile) as fh:
                return fh.read()
        return ''
    
    def _cleanup(self):
        """Stop the redis-server for this instance if it's running."""
        if not self.pid:
            return
        
        logger.debug('Shutting down redis server with pid of %r', self.pid)
        
        try:
            # Try graceful shutdown
            if self._connection:
                self._connection.shutdown(save=True, now=True)
            
            # Wait for process to exit
            import psutil
            try:
                process = psutil.Process(self.pid)
                for i in range(50):
                    if not process.is_running():
                        break
                    time.sleep(.2)
                
                # Force kill if still running
                if process.is_running():
                    logger.warning('Redis graceful shutdown failed, forcefully killing pid %r', self.pid)
                    import signal
                    os.kill(self.pid, signal.SIGKILL)
            except psutil.NoSuchProcess:
                pass
        except Exception as e:
            logger.debug('Error during cleanup: %s', e)
        
        # Clean up socket file
        self.socket_file = None
        
        # Clean up temporary directory
        if self.redis_dir and os.path.isdir(self.redis_dir):
            shutil.rmtree(self.redis_dir)
        
        # Clean up registry file
        if self.settingregistryfile and os.path.exists(self.settingregistryfile):
            os.remove(self.settingregistryfile)
            self.settingregistryfile = None
        
        self.running = False
        self.redis_dir = None
        self.pidfile = None
    
    def __del__(self):
        """Cleanup on deletion."""
        self._cleanup()

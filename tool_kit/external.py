import atexit
from contextlib import contextmanager
import logging
import os
from typing import Optional

import sentry_sdk
from sshtunnel import SSHTunnelForwarder
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


class ErrorTracking:
    @classmethod
    def initialize(cls):
        sentry_dsn = os.environ.get('SENTRY_DSN')
        if sentry_dsn:
            sentry_sdk.init(sentry_dsn)
        else:
            raise Exception('Unable to load DSN')


class DatabaseConnection:
    def __init__(
            self,
            host: str = 'localhost',
            username: str = os.environ['DB_USERNAME'],
            password: str = os.environ['DB_PASSWORD'],
            database_name: str = os.environ['DB_NAME'],
            port: int = os.environ.get('DB_PORT'),
            ssl_tunnel: 'SshTunnel' = None,
            db_protocol: str = 'postgresql'
    ):
        self.engine = None
        self.session_factory = None

        self._init_connection(
            host=host,
            db_user=username,
            db_password=password,
            db_name=database_name,
            port=port,
            ssl_tunnel=ssl_tunnel,
            db_protocol=db_protocol
        )

    @contextmanager
    def get_new_session(self):
        session = self.session_factory()
        try:
            yield session
            session.commit()
        finally:
            session.close()

    def _init_connection(
            self, host, db_user, db_password, db_name, port, ssl_tunnel, db_protocol
    ) -> (sessionmaker, None):
        if not port and not ssl_tunnel:
            raise Exception('We need either a port or an ssl tunnel to connect to.')

        if ssl_tunnel:
            port = ssl_tunnel.get_entrance_port()

        self.engine = create_engine(f'{db_protocol}://{db_user}:{db_password}@{host}:{port}/{db_name}')
        self.session_factory = sessionmaker(bind=self.engine, expire_on_commit=False)


class SshTunnel:
    def __init__(
            self,
            proxy_target_port,
            host=os.environ['SSH_HOST'],
            username=os.environ['SSH_USERNAME'],
            key_file_path=os.environ.get('SSH_KEY_FILE'),
            password=os.environ.get('SSH_PASSWORD'),
            remote_host='127.0.0.1'
    ):
        self._remote_host = remote_host
        self._remote_port = proxy_target_port
        self._ssh_host = host
        self._ssh_port = 22

        self._ssh_username = username
        self._key_file_path = key_file_path
        self._password = password

        self._tunneler: Optional[SSHTunnelForwarder] = None

    def close_ssh_tunnel(self):
        if self._tunneler:
            logging.info('Closing ssh tunnel')
            self._tunneler.stop()
            self._tunneler = None

    def get_entrance_port(self):
        """
        Starts the tunnel if it's not already started, and returns the local port to connect to for the proxy/tunnel
        """
        if not self._tunneler:
            parameters = {
                'ssh_address_or_host': (self._ssh_host, self._ssh_port),
                'ssh_username': self._ssh_username,
                'remote_bind_address': (self._remote_host, self._remote_port)
            }
            if self._key_file_path:
                parameters['ssh_pkey'] = self._key_file_path
            else:
                parameters['ssh_password'] = self._password

            self._tunneler = SSHTunnelForwarder(**parameters)
            self._tunneler.start()
            atexit.register(self.close_ssh_tunnel)

        return self._tunneler.ssh_port


class Environment:
    @classmethod
    def is_dev(cls) -> bool:
        return bool(os.environ.get('IS_DEV', False))

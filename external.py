import atexit
from contextlib import contextmanager
from enum import Enum
import logging
import os
from typing import Optional

import sentry_sdk
from sshtunnel import SSHTunnelForwarder
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


class Environment(Enum):
    PROD = 1
    DEV = 2


CURRENT_ENV = Environment.DEV


class ErrorTracking:
    @classmethod
    def initialize(cls):
        if CURRENT_ENV == Environment.PROD:
            sentry_dsn = os.environ.get('SENTRY_DSN')
            if sentry_dsn:
                sentry_sdk.init(sentry_dsn)
            else:
                raise Exception('Unable to load DSN')
        else:
            logging.warning('Skipping error tracking service')


class Database:
    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, '_instance'):
            instance = super(Database, cls).__new__(cls)

            instance.engine = None
            instance.session_factory = None
            instance._init_connection()

            cls._instance = instance

        return getattr(cls, '_instance')

    @contextmanager
    def get_new_session(self):
        session = self.session_factory()
        try:
            yield session
            session.commit()
        finally:
            session.close()

    def _init_connection(self) -> (sessionmaker, None):
        db_user = os.environ.get('DB_USERNAME')
        db_password = os.environ.get('DB_PASSWORD')
        if not (db_user and db_password):
            raise Exception('Database credentials not set')

        db_name = os.environ.get('DB_NAME')
        if not db_name:
            raise Exception('Database name not set')

        db_connection_port = 5432
        if CURRENT_ENV == Environment.DEV:
            tunnel = SshTunnelFactory().start_tunnel()
            db_connection_port = tunnel.local_bind_port

        self.engine = create_engine(f'postgresql://{db_user}:{db_password}@localhost:{db_connection_port}/{db_name}')
        self.session_factory = sessionmaker(bind=self.engine, expire_on_commit=False)


class SshTunnelFactory:
    def __init__(self):
        self.remote_host = '127.0.0.1'
        self.remote_port = 5432
        self.ssh_host = os.environ.get('SSH_HOST')
        self.ssh_port = 22
        self.ssh_username = os.environ.get('SSH_USERNAME')
        self.ssh_pem_file = os.environ.get('SSH_KEY_FILE')
        if not (self.ssh_host and self.ssh_username and self.ssh_pem_file):
            raise Exception('SSH credentials not set')

        self.ssh_tunnel: Optional[SSHTunnelForwarder] = None

    def close_ssh_tunnel(self):
        if self.ssh_tunnel:
            logging.info('Closing ssh tunnel')
            self.ssh_tunnel.stop()
            self.ssh_tunnel = None

    def start_tunnel(self):
        self.ssh_tunnel = SSHTunnelForwarder(
            ssh_address_or_host=(self.ssh_host, self.ssh_port),
            ssh_username=self.ssh_username,
            ssh_pkey=self.ssh_pem_file,
            remote_bind_address=(self.remote_host, self.remote_port)
        )
        self.ssh_tunnel.start()
        atexit.register(self.close_ssh_tunnel)

        return self.ssh_tunnel

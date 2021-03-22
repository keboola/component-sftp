'''
Created on 10. 10. 2018

@author: david esner
'''
import logging
import os
from datetime import datetime
from io import StringIO
from pathlib import Path
from typing import Callable

import backoff
import paramiko
from keboola.component import CommonInterface

MAX_RETRIES = 5

KEY_USER = 'user'
KEY_PASSWORD = '#pass'
KEY_HOSTNAME = 'hostname'
KEY_PORT = 'port'
KEY_REMOTE_PATH = 'path'
KEY_APPENDDATE = 'append_date'
KEY_PRIVATE_KEY = '#private_key'

KEY_DEBUG = 'debug'
PASS_GROUP = [KEY_PRIVATE_KEY, KEY_PASSWORD]

REQUIRED_PARAMETERS = [KEY_USER, PASS_GROUP,
                       KEY_HOSTNAME, KEY_REMOTE_PATH, KEY_PORT]

REQUIRED_IMAGE_PARS = []

APP_VERSION = '1.0.0'


def get_local_data_path():
    return Path(__file__).resolve().parent.parent.joinpath('data').as_posix()


def get_data_folder_path():
    data_folder_path = None
    if not os.environ.get('KBC_DATADIR'):
        data_folder_path = get_local_data_path()
    return data_folder_path


def connect_to_server(port, host, user, password, pkey):
    conn = paramiko.Transport((host, port))
    try:
        conn.connect(username=user, password=password, pkey=pkey)
    except paramiko.ssh_exception.AuthenticationException:
        logging.error('Authentication failed: recheck your authentication parameters')
        exit(1)
    sftp = paramiko.SFTPClient.from_transport(conn)
    return sftp, conn


class Component(CommonInterface):
    def __init__(self):
        # for easier local project setup
        data_folder_path = get_data_folder_path()
        super().__init__(data_folder_path=data_folder_path)
        try:
            self.validate_configuration(REQUIRED_PARAMETERS)
            self.validate_image_parameters(REQUIRED_IMAGE_PARS)
        except ValueError as err:
            logging.exception(err)
            exit(1)
        if self.configuration.parameters.get(KEY_DEBUG):
            self.set_debug_mode()

    @staticmethod
    def set_debug_mode():
        logging.getLogger().setLevel(logging.DEBUG)
        logging.info('Running version %s', APP_VERSION)
        logging.info('Loading configuration...')

    def run(self):
        '''
        Main execution code
        '''
        params = self.configuration.parameters
        pkey = None

        if params[KEY_PRIVATE_KEY]:
            keyfile = StringIO(params[KEY_PRIVATE_KEY])
            try:
                pkey = _parse_private_key(keyfile)
            except (paramiko.SSHException, IndexError):
                logging.error("Private Key is invalid")
                exit(1)

        sftp, conn = connect_to_server(params[KEY_PORT],
                                       params[KEY_HOSTNAME],
                                       params[KEY_USER],
                                       params[KEY_PASSWORD],
                                       pkey)

        in_tables = self.get_in_tables()
        in_files_per_tag = self.get_input_file_definitions_grouped_by_tag_group(only_latest_files=True)
        in_file_names = [self.files_in_path + "/" + item.name for sublist
                         in in_files_per_tag.values() for item in sublist]

        for fl in in_tables + in_file_names:
            self._upload_file(fl, sftp)

        sftp.close()
        conn.close()
        logging.info("Done.")

    def get_in_tables(self):
        return [os.path.join(self.tables_in_path, f) for f in os.listdir(self.tables_in_path)
                if os.path.isfile(os.path.join(self.tables_in_path, f))
                and not os.path.join(self.tables_in_path, f).endswith('.manifest') and not os.path.join(
                self.tables_in_path, f).endswith('.DS_Store')]

    def _upload_file(self, input_file, sftp):
        params = self.configuration.parameters
        now = ''
        if params.get(KEY_APPENDDATE):
            now = "_" + str(datetime.utcnow().strftime('%Y%m%d%H%M%S'))

        path = params[KEY_REMOTE_PATH]
        if not path[-1] == "/":
            path = path + "/"

        filename, file_extension = os.path.splitext(os.path.basename(input_file))
        destination = path + filename + now + file_extension
        logging.info("File Source: %s", input_file)
        logging.info("File Destination: %s", destination)
        try:
            self._try_to_execute_sftp_operation(sftp.put, input_file, destination)
        except FileNotFoundError:
            logging.exception(
                f"Destination path: '{path}' in SFTP Server not found, recheck the remote destination path")
            exit(1)
        except PermissionError:
            logging.exception(f"Permission Error: you do not have permissions to write to '{path}', "
                              f"choose a different directory on the SFTP server")
            exit(1)

    @backoff.on_exception(backoff.expo,
                          ConnectionError,
                          max_tries=MAX_RETRIES)
    def _try_to_execute_sftp_operation(self, operation: Callable, *args):
        return operation(*args)


def _parse_private_key(keyfile):
    # try all versions of encryption keys
    pkey = None
    failed = False
    try:
        pkey = paramiko.RSAKey.from_private_key(keyfile)
    except paramiko.SSHException:
        logging.warning("RSS Private key invalid, trying DSS.")
        failed = True
    # DSS
    if failed:
        try:
            pkey = paramiko.DSSKey.from_private_key(keyfile)
            failed = False
        except (paramiko.SSHException, IndexError):
            logging.warning("DSS Private key invalid, trying ECDSAKey.")
            failed = True
    # ECDSAKey
    if failed:
        try:
            pkey = paramiko.ECDSAKey.from_private_key(keyfile)
            failed = False
        except (paramiko.SSHException, IndexError):
            logging.warning("ECDSAKey Private key invalid, trying Ed25519Key.")
            failed = True
    # Ed25519Key
    if failed:
        try:
            pkey = paramiko.Ed25519Key.from_private_key(keyfile)
        except (paramiko.SSHException, IndexError) as e:
            logging.warning("Ed25519Key Private key invalid.")
            raise e

    return pkey


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        comp.run()
    except Exception as exc:
        logging.exception(exc)
        exit(2)

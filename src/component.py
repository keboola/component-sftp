'''
Created on 10. 10. 2018

@author: david esner
'''
import glob
import json
import logging
import os
import sys
from datetime import datetime
from io import StringIO
from pathlib import Path
from typing import Callable

import backoff
import paramiko
from kbc.env_handler import KBCEnvHandler

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

MANDATORY_PARS = [KEY_USER, PASS_GROUP,
                  KEY_HOSTNAME, KEY_REMOTE_PATH, KEY_PORT]

APP_VERSION = '0.0.3'


class Component(KBCEnvHandler):

    def __init__(self, debug=False):
        # for easier local project setup
        default_data_dir = Path(__file__).resolve().parent.parent.joinpath('data').as_posix() \
            if not os.environ.get('KBC_DATADIR') else None

        KBCEnvHandler.__init__(self, MANDATORY_PARS, log_level=logging.DEBUG if debug else logging.INFO,
                               data_path=default_data_dir)
        # override debug from config
        if self.cfg_params.get(KEY_DEBUG):
            debug = True
        if debug:
            logging.getLogger().setLevel(logging.DEBUG)
        logging.info('Running version %s', APP_VERSION)
        logging.info('Loading configuration...')

        try:
            self.validate_config(MANDATORY_PARS)
        except ValueError as e:
            logging.exception(e)
            exit(1)

        self.files_in_path = os.path.join(self.data_path, 'in', 'files')

    def run(self):
        '''
        Main execution code
        '''
        params = self.cfg_params  # noqa
        pkey = None
        if params[KEY_PRIVATE_KEY]:
            keyfile = StringIO(params[KEY_PRIVATE_KEY])
            pkey = self._parse_private_key(keyfile)
        # ## SFTP Connection ###
        port = params[KEY_PORT]
        conn = paramiko.Transport((params[KEY_HOSTNAME], port))

        conn.connect(username=params[KEY_USER], password=params[KEY_PASSWORD], pkey=pkey)
        sftp = paramiko.SFTPClient.from_transport(conn)

        in_tables = [os.path.join(self.tables_in_path, f) for f in os.listdir(self.tables_in_path)
                     if os.path.isfile(os.path.join(self.tables_in_path, f))
                     and not os.path.join(self.tables_in_path, f).endswith('.manifest') and not os.path.join(
                self.tables_in_path, f).endswith('.DS_Store')]  # noqa

        files_per_tag_groups = self.get_files_per_tag_groups(files=self.get_all_files())
        latest_files = self.get_latest_files(files_per_tag_groups)
        in_files = self._drop_id_from_filename(latest_files)

        for fl in in_tables + in_files:

            now = ''
            if params.get(KEY_APPENDDATE):
                now = "_" + str(datetime.utcnow().strftime('%Y%m%d%H%M%S'))
            filename, file_extension = os.path.splitext(os.path.basename(fl))
            destination = params[KEY_REMOTE_PATH] + filename + now + file_extension
            logging.info("File Source: %s", fl)
            logging.info("File Destination: %s", destination)
            self._try_to_execute_sftp_operation(sftp.put, fl, destination)

        sftp.close()
        conn.close()
        logging.info("Done.")

    @backoff.on_exception(backoff.expo,
                          IOError,
                          max_tries=MAX_RETRIES)
    def _try_to_execute_sftp_operation(self, operation: Callable, *args):
        return operation(*args)

    def _parse_private_key(self, keyfile):
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
            except paramiko.SSHException:
                logging.warning("DSS Private key invalid, trying ECDSAKey.")
                failed = True
        # ECDSAKey
        if failed:
            try:
                pkey = paramiko.ECDSAKey.from_private_key(keyfile)
                failed = False
            except paramiko.SSHException:
                logging.warning("ECDSAKey Private key invalid, trying Ed25519Key.")
                failed = True
        # Ed25519Key
        if failed:
            try:
                pkey = paramiko.Ed25519Key.from_private_key(keyfile)
            except paramiko.SSHException as e:
                logging.warning("Ed25519Key Private key invalid.")
                raise e

        return pkey

    def get_all_files(self):
        glob_files = os.path.join(self.files_in_path, '*')
        return [path_name for path_name in glob.glob(glob_files) if
                not (path_name.endswith('.manifest') | path_name.endswith('.DS_Store'))]

    def get_files_per_tag_groups(self, files) -> dict:
        files_per_tag = {}
        for f in files:
            manifest_path = f + '.manifest'
            with open(manifest_path) as manFile:
                tag_group_v1 = json.load(manFile)['tags']
                tag_group_v1.sort()
                tag_group_key = ','.join(tag_group_v1)
                if not files_per_tag.get(tag_group_key):
                    files_per_tag[tag_group_key] = []
                files_per_tag[tag_group_key].append(f)
        return files_per_tag

    def get_latest_files(self, files_per_tag_groups):
        files_to_process = list()
        for tag_group in files_per_tag_groups:
            max_filename = ''
            max_id = ''
            max_timestamp = '0'
            for f in files_per_tag_groups[tag_group]:
                manifest_path = f + '.manifest'
                with open(manifest_path) as manFile:
                    man_json = json.load(manFile)
                    creation_date = man_json['created']
                if creation_date > max_timestamp:
                    max_timestamp = creation_date
                    max_id = man_json['id']
                    max_filename = f
            files_to_process.append({"file_name": max_filename, "id": max_id})
        return files_to_process

    def _drop_id_from_filename(self, latest_files):
        file_paths = []
        for f in latest_files:
            file_id = f"{f['id']}_"
            file_name = Path(f['file_name']).name
            if file_name.startswith(file_id):
                old_file_name = f['file_name']
                f['file_name'] = f"{os.path.join(os.path.dirname(f['file_name']), file_name.split(file_id)[1])}"
                os.rename(old_file_name, f['file_name'])
            file_paths.append(f['file_name'])
        return file_paths


"""
        Main entrypoint
"""
if __name__ == "__main__":
    if len(sys.argv) > 1:
        debug_arg = sys.argv[1]
    else:
        debug_arg = False
    try:
        comp = Component(debug_arg)
        comp.run()
    except Exception as exc:
        logging.exception(exc)
        exit(1)

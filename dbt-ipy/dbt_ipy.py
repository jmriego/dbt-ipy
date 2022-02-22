import argparse
import atexit
import base64
import json
import os
import shlex
import signal
import socket
import subprocess
import time
from typing import Any, Dict, List, Optional, Union
import tempfile

from IPython.core.magic import Magics, cell_magic, line_magic, magics_class
from agate import Table
import requests

class Sql(str):
    def _repr_pretty_(self, p, cycle):
        p.text(str(self) if not cycle else '...')

class ServerProcess():
    def __init__(
        self,
        cmd_args=[]
    ):
        self.error = None
        self.criteria = ('ready',)
        self.cmd = ['dbt'] + cmd_args
        parser = argparse.ArgumentParser()
        parser.add_argument('--port', default=8580, type=int)
        parser.add_argument('--existing', action='store_true')
        args, _ = parser.parse_known_args(cmd_args[1:]) #remove rpc argument
        self.port = args.port
        self.existing = args.existing

    def run(self):
        self.proc = subprocess.Popen(self.cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        return self.proc

    def start(self):
        if not self.existing:
            self.run()
        for _ in range(30):
            if self.is_up():
                break
            time.sleep(0.5)
        if not self.can_connect():
            raise RuntimeError('server never appeared!')
        status_result = self.query(
            {'method': 'status', 'id': 1, 'jsonrpc': '2.0'}
        ).json()
        if not self._compare_result(status_result):
            raise RuntimeError(
                'Got invalid status result: {}'.format(status_result)
            )

    def stop(self):
        self.proc.terminate()
        self.proc.wait()

    def can_connect(self):
        sock = socket.socket()
        try:
            sock.connect(('localhost', self.port))
        except socket.error:
            return False
        finally:
            sock.close()
        return True

    def _compare_result(self, result):
        return result['result']['state'] in self.criteria

    def status_ok(self):
        result = self.query(
            {'method': 'status', 'id': 1, 'jsonrpc': '2.0'}
        ).json()
        return self._compare_result(result)

    def is_up(self):
        if not self.can_connect():
            return False
        return self.status_ok()

    @property
    def url(self):
        return 'http://localhost:{}/jsonrpc'.format(self.port)

    def query(self, query):
        headers = {'content-type': 'application/json'}
        return requests.post(self.url, headers=headers, data=json.dumps(query))


class Querier:
    def __init__(self, server: ServerProcess):
        self.server = server

    def sighup(self):
        os.kill(self.server.proc.pid, signal.SIGHUP)

    def build_request_data(self, method, params, request_id):
        return {
            'jsonrpc': '2.0',
            'method': method,
            'params': params,
            'id': request_id,
        }

    def request(self, method, params=None, request_id=1):
        if params is None:
            params = {}

        data = self.build_request_data(
            method=method, params=params, request_id=request_id
        )
        response = self.server.query(data)
        assert response.ok, f'invalid response from server: {response.text}'
        return response.json()

    def status(self, request_id: int = 1):
        return self.request(method='status', request_id=request_id)

    def wait_for_status(self, expected, times=30) -> bool:
        for _ in range(times):
            time.sleep(0.5)
            status = self.is_result(self.status())
            if status['state'] == expected:
                return True
        return False

    def ps(self, active=True, completed=False, request_id=1):
        params = {}
        if active is not None:
            params['active'] = active
        if completed is not None:
            params['completed'] = completed

        return self.request(method='ps', params=params, request_id=request_id)

    def kill(self, task_id: str, request_id: int = 1):
        params = {'task_id': task_id}
        return self.request(
            method='kill', params=params, request_id=request_id
        )

    def poll(
        self,
        request_token: str,
        logs: Optional[bool] = None,
        logs_start: Optional[int] = None,
        request_id: int = 1,
    ):
        params = {
            'request_token': request_token,
        }
        if logs is not None:
            params['logs'] = logs
        if logs_start is not None:
            params['logs_start'] = logs_start
        return self.request(
            method='poll', params=params, request_id=request_id
        )

    def gc(
        self,
        task_ids: Optional[List[str]] = None,
        before: Optional[str] = None,
        settings: Optional[Dict[str, Any]] = None,
        request_id: int = 1,
    ):
        params = {}
        if task_ids is not None:
            params['task_ids'] = task_ids
        if before is not None:
            params['before'] = before
        if settings is not None:
            params['settings'] = settings
        return self.request(
            method='gc', params=params, request_id=request_id
        )

    def cli_args(self, cli: str, request_id: int = 1):
        return self.request(
            method='cli_args', params={'cli': cli}, request_id=request_id
        )

    def deps(self, request_id: int = 1):
        return self.request(method='deps', request_id=request_id)

    def compile(
        self,
        models: Optional[Union[str, List[str]]] = None,
        exclude: Optional[Union[str, List[str]]] = None,
        threads: Optional[int] = None,
        request_id: int = 1,
    ):
        params = {}
        if models is not None:
            params['models'] = models
        if exclude is not None:
            params['exclude'] = exclude
        if threads is not None:
            params['threads'] = threads
        return self.request(
            method='compile', params=params, request_id=request_id
        )

    def run(
        self,
        models: Optional[Union[str, List[str]]] = None,
        exclude: Optional[Union[str, List[str]]] = None,
        threads: Optional[int] = None,
        request_id: int = 1,
    ):
        params = {}
        if models is not None:
            params['models'] = models
        if exclude is not None:
            params['exclude'] = exclude
        if threads is not None:
            params['threads'] = threads
        return self.request(
            method='run', params=params, request_id=request_id
        )

    def run_operation(
        self,
        macro: str,
        args: Optional[Dict[str, Any]],
        request_id: int = 1,
    ):
        params = {'macro': macro}
        if args is not None:
            params['args'] = args
        return self.request(
            method='run-operation', params=params, request_id=request_id
        )

    def seed(
        self,
        select: Optional[Union[str, List[str]]] = None,
        exclude: Optional[Union[str, List[str]]] = None,
        show: bool = None,
        threads: Optional[int] = None,
        request_id: int = 1,
    ):
        params = {}
        if select is not None:
            params['select'] = select
        if exclude is not None:
            params['exclude'] = exclude
        if show is not None:
            params['show'] = show
        if threads is not None:
            params['threads'] = threads
        return self.request(
            method='seed', params=params, request_id=request_id
        )

    def snapshot(
        self,
        select: Optional[Union[str, List[str]]] = None,
        exclude: Optional[Union[str, List[str]]] = None,
        threads: Optional[int] = None,
        request_id: int = 1,
    ):
        params = {}
        if select is not None:
            params['select'] = select
        if exclude is not None:
            params['exclude'] = exclude
        if threads is not None:
            params['threads'] = threads
        return self.request(
            method='snapshot', params=params, request_id=request_id
        )

    def snapshot_freshness(
        self,
        select: Optional[Union[str, List[str]]] = None,
        threads: Optional[int] = None,
        request_id: int = 1,
    ):
        params = {}
        if select is not None:
            params['select'] = select
        if threads is not None:
            params['threads'] = threads
        return self.request(
            method='snapshot-freshness', params=params, request_id=request_id
        )

    def test(
        self,
        models: Optional[Union[str, List[str]]] = None,
        exclude: Optional[Union[str, List[str]]] = None,
        threads: Optional[int] = None,
        data: bool = None,
        schema: bool = None,
        request_id: int = 1,
    ):
        params = {}
        if models is not None:
            params['models'] = models
        if exclude is not None:
            params['exclude'] = exclude
        if data is not None:
            params['data'] = data
        if schema is not None:
            params['schema'] = schema
        if threads is not None:
            params['threads'] = threads
        return self.request(
            method='test', params=params, request_id=request_id
        )

    def docs_generate(self, compile: bool = None, request_id: int = 1):
        params = {}
        if compile is not None:
            params['compile'] = True
        return self.request(
            method='docs.generate', params=params, request_id=request_id
        )

    def compile_sql(
        self,
        sql: str,
        name: str = 'test_compile',
        macros: Optional[str] = None,
        request_id: int = 1,
    ):
        sql = base64.b64encode(sql.encode('utf-8')).decode('utf-8')
        params = {
            'name': name,
            'sql': sql,
            'macros': macros,
        }
        return self.request(
            method='compile_sql', params=params, request_id=request_id
        )

    def run_sql(
        self,
        sql: str,
        name: str = 'test_run',
        macros: Optional[str] = None,
        request_id: int = 1,
    ):
        sql = base64.b64encode(sql.encode('utf-8')).decode('utf-8')
        params = {
            'name': name,
            'sql': sql,
            'macros': macros,
        }
        return self.request(
            method='run_sql', params=params, request_id=request_id
        )

    def get_manifest(self, request_id=1):
        return self.request(
            method='get-manifest', params={}, request_id=request_id
        )

    def is_result(self, data: Dict[str, Any], id=None) -> Dict[str, Any]:
        if id is not None:
            assert data['id'] == id
        assert data['jsonrpc'] == '2.0'
        if 'error' in data or 'result' not in data:
            raise RuntimeError(
                'Got invalid response: {}'.format(data)
            )
        else:
            return data['result']

    def is_async_result(self, data: Dict[str, Any], id=None) -> str:
        result = self.is_result(data, id)
        assert 'request_token' in result
        return result['request_token']

    def is_error(self, data: Dict[str, Any], id=None) -> Dict[str, Any]:
        if id is not None:
            assert data['id'] == id
        assert data['jsonrpc'] == '2.0'
        assert 'result' not in data
        assert 'error' in data
        return data['error']

    def async_wait(
        self, token: str, timeout: int = 60, state='success'
    ) -> Dict[str, Any]:
        start = time.time()
        while True:
            time.sleep(0.5)
            response = self.poll(token)
            if 'error' in response:
                return response
            result = self.is_result(response)
            assert 'state' in result
            if result['state'] == state:
                return response
            delta = (time.time() - start)
            assert timeout > delta, \
                f'At time {delta}, never saw {state}.\nLast response: {result}'

    def async_wait_for_result(self, data: Dict[str, Any], state='success'):
        token = self.is_async_result(data)
        return self.is_result(self.async_wait(token, state=state))

    def async_wait_for_error(self, data: Dict[str, Any], state='success'):
        token = self.is_async_result(data)
        return self.is_error(self.async_wait(token, state=state))

@magics_class
class DBTMagics(Magics):
    "Magics that hold additional state"

    def __init__(self, shell):
        # You must call the parent constructor
        super(DBTMagics, self).__init__(shell)
        atexit.register(self._stop)
        self.last_result = None

    def _stop(self):
        try:
            self.querier.server.stop()
        except AttributeError:
            pass

    @line_magic
    def dbt(self, line):
        args = shlex.split(line)
        if args[0] == 'rpc':
            self._stop()
            proc = ServerProcess(args)
            self.querier = Querier(proc)
            try:
                self.querier.server.start()
            except Exception:
                return self.querier.server.proc.stdout.read().decode('utf-8')

            return(self.querier)

    @cell_magic
    def run_sql(self, line, cell):
        max_rows = 500
        query = "select * from ({}) dbt_run_sql limit {}".format(cell, max_rows)
        resp = self.querier.run_sql(query)
        result = self.querier.async_wait_for_result(resp)
        resp_table = result['results'][0]['table']
        column_names = resp_table['column_names']
        rows_dict = [dict(zip(column_names, row)) for row in resp_table['rows']]
        table_result = Table.from_object(rows_dict)
        table_result.print_table()
        self.last_result = table_result
        return table_result

    @cell_magic
    def compile_sql(self, line, cell):
        query = cell
        resp = self.querier.compile_sql(query)
        result = self.querier.async_wait_for_result(resp)
        compiled_sql = result['results'][0]['compiled_sql']
        self.last_result = compiled_sql
        return Sql(compiled_sql)

    @line_magic
    def dbt_clipboard(self, line):
        import pyperclip
        if isinstance(self.last_result, Table):
            try:
                fd, path = tempfile.mkstemp()
                self.last_result.to_csv(path)
                with open(path) as f:
                    pyperclip.copy(f.read())
            finally:
                os.remove(path)
        else:
            pyperclip.copy(self.last_result)

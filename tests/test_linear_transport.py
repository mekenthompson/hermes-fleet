"""A desktop/API handler reaches one gateway owner without a local service."""
from __future__ import annotations
import contextvars,json,multiprocessing,os,socket,tempfile,unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from linear_fake_api import load_plugin
plugin = load_plugin()
from hermes_fleet_linear_plugin import transport

# Spawn provides a separate plugin closure/process, matching desktop vs gateway.
def gateway_process(home, ready, stop):
    server = transport.Server(Path(home), 'alpha', lambda args, context:
        json.dumps({'ok': True, 'context': vars(context), 'args': args, 'pid': os.getpid()}), lambda session: None)
    ready.set();stop.wait(10);server.close()

class LinearTransportTests(unittest.TestCase):
    def test_separate_process_preserves_host_context_and_generation(self):
        with tempfile.TemporaryDirectory() as tmp:
            home=Path(tmp);ctx=multiprocessing.get_context('spawn');ready=ctx.Event();stop=ctx.Event()
            worker=ctx.Process(target=gateway_process,args=(str(home),ready,stop));worker.start()
            try:
                self.assertTrue(ready.wait(5))
                context=SimpleNamespace(profile='alpha',platform='api_server',session_key='host-key',session_id='host-id',run_generation=9)
                result=json.loads(transport.chat_request(home,{'action':'start','issue':'ABC-1','profile':'spoof','run_generation':500},context))
                self.assertNotEqual(result['pid'],os.getpid())
                self.assertEqual(result['context'],vars(context))
                self.assertEqual(result['context']['run_generation'],9)
                self.assertEqual(transport.request(home,{'op':'status'})['pid'],worker.pid)
            finally:
                stop.set();worker.join(5)
                if worker.is_alive():worker.terminate();worker.join()
            self.assertEqual(worker.exitcode,0)
            self.assertFalse(transport.endpoint(home).exists())

    def test_tool_registration_without_local_service_uses_existing_gateway(self):
        class Context:
            def get_config(self,key,default=None):return True if key=='enabled' else default
            def register_tool(self,**kwargs):self.tool=kwargs['handler']
            def register_hook(self,*args):pass
            def register_profile_service(self,*args):self.service=args
        context=Context();plugin.register(context)
        with tempfile.TemporaryDirectory() as tmp:
            home=Path(tmp);seen=[]
            server=transport.Server(home,'alpha',lambda args,host: seen.append(host.session_key) or json.dumps({'ok':True}),lambda session:None)
            try:
                with patch.dict('sys.modules',{'hermes_constants':SimpleNamespace(get_hermes_home=lambda:home)}):
                    result=context.tool({'action':'start','issue':'ABC-1'},SimpleNamespace(session_key='native-chat',session_id='native-id',profile='alpha',platform='api_server',run_generation=3))
                self.assertTrue(json.loads(result)['ok']);self.assertEqual(seen,['native-chat'])
            finally:server.close()

    def test_foreign_home_and_duplicate_listener_are_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            home=Path(tmp);seen=[]
            server=transport.Server(home,'alpha',lambda *args:seen.append(args),lambda session:seen.append(session))
            try:
                with self.assertRaises(transport.Unavailable):transport.Server(home,'alpha',lambda *args:None,lambda session:None)
                with self.assertRaises(transport.Uncertain):transport.request(home,{'op':'turn_end','home':'/foreign','session_id':'forged'})
                self.assertEqual(seen,[])
            finally:server.close()

    def test_scoped_context_is_carried_into_rpc_handler(self):
        scope=contextvars.ContextVar('profile-scope',default='foreign')
        with tempfile.TemporaryDirectory() as tmp:
            home=Path(tmp);token=scope.set('alpha')
            server=transport.Server(home,'alpha',lambda args,ctx:scope.get(),lambda session:None)
            scope.reset(token)
            try:self.assertEqual(transport.chat_request(home,{},SimpleNamespace()),'alpha')
            finally:server.close()

    def test_unsafe_paths_and_missing_service_do_not_create_an_executor(self):
        with tempfile.TemporaryDirectory() as tmp:
            home=Path(tmp)
            with self.assertRaises(transport.Unavailable):transport.request(home,{'op':'status'})
            transport.endpoint(home).parent.mkdir(mode=0o755)
            with self.assertRaises(transport.Unavailable):transport.Server(home,'alpha',lambda *args:None,lambda session:None)
            transport.endpoint(home).parent.chmod(0o700);transport.endpoint(home).write_text('not a socket')
            with self.assertRaises(transport.Unavailable):transport.Server(home,'alpha',lambda *args:None,lambda session:None)

if __name__=='__main__':unittest.main()

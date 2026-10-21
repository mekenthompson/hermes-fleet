"""Optional tools for discovery and narrowly mediated Claude.ai connector calls.

The native ACP transport integration is deliberately experimental; do not infer
connector success from assistant prose. Only transport-correlated raw output counts.
"""
from __future__ import annotations
import json
import re

_PREFIX = 'mcp__claude_ai_'
_REF = re.compile(r'^mcp__claude_ai_[A-Za-z0-9_-]{1,64}__[A-Za-z0-9_-]{1,64}$')

def _refs(result):
    if not isinstance(result, dict): return []
    output = result.get('rawOutput')
    if not isinstance(output, list): return []
    refs=[]
    for row in output:
        if not isinstance(row, dict): continue
        name=row.get('name') or row.get('toolName')
        if isinstance(name,str) and _REF.fullmatch(name): refs.append(name)
    return sorted(set(refs))

class ConnectorTools:
    def __init__(self, *, search, invoke): self.search, self.invoke = search, invoke
    def claude_connector_search(self, query):
        if not isinstance(query,str) or not query.strip(): return {'error':'query_required'}
        result=self.search(query.strip())
        return {'references':_refs(result), 'note':'Discovery only; profile policy still authorizes calls.'}
    def claude_connector_call(self, tool, arguments):
        if not isinstance(tool,str) or not _REF.fullmatch(tool) or not isinstance(arguments,dict):
            return {'error':'invalid_connector_reference_or_arguments'}
        try: result=self.invoke(tool,arguments)
        except TimeoutError: return {'status':'unknown_outcome','retry':False,'reason':'timeout_after_approval'}
        except Exception: return {'status':'unknown_outcome','retry':False,'reason':'transport_lost_after_approval'}
        # Never reinterpret model-authored text as a connector result.
        raw=result.get('rawOutput') if isinstance(result,dict) else None
        if isinstance(result, dict) and result.get('execution') == 'not_started':
            return {'status':'no_execution','retry':False,'reason':'native_tool_event_not_observed'}
        return {'status':'completed','rawOutput':raw} if isinstance(raw,(dict,list,str,int,float,bool,type(None))) and 'rawOutput' in result else {'status':'no_execution','retry':False,'reason':'no_correlated_native_output'}

def register(ctx, settings=None):
    settings=settings if isinstance(settings,dict) else {}
    if settings.get('enabled') is not True: return
    backend=settings.get('_backend')
    if not callable(backend): raise ValueError('enabled connector plugin requires host backend binding')
    tools=ConnectorTools(search=lambda q: backend('search',q), invoke=lambda t,a: backend('call',t,a))
    ctx.register_tool(name='claude_connector_search',toolset='claude_account_connectors',schema={'type':'object','properties':{'query':{'type':'string'}},'required':['query'],'additionalProperties':False},handler=tools.claude_connector_search)
    ctx.register_tool(name='claude_connector_call',toolset='claude_account_connectors',schema={'type':'object','properties':{'tool':{'type':'string'},'arguments':{'type':'object'}},'required':['tool','arguments'],'additionalProperties':False},handler=tools.claude_connector_call)

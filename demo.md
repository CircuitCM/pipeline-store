# Usage

Pipelines in this library use common notation, such as LangChain or R. However it differs from LangChain and other Pipeline libs through certain flexible differences.

```python
import json
from collections.abc import Callable, Sequence

#from pydantic import PrivateAttr

from pipeline import Pipeline,Step,pipe_funcdict
import asyncio as aio
import time as tm

def s1(n,a=None):
    n.append(a)
    print(n)
    return n
    
class SimplePipe(Pipeline):
    _functions: dict[str, tuple[Callable,tuple]] =pipe_funcdict(s1)

print(SimplePipe._functions)
    
#Can be defined in various ways
t1= SimplePipe.new('Simple 1') | Step(s1, a=1) | Step(s1, a=2) | Step(s1, a=3)
t2=SimplePipe(name='Simple 2').step(s1,a=4).step(s1,a=5).step(s1,a=6)
t3=SimplePipe(name='Simple 3',steps=[Step(s1,a=7),Step(s1,a=8),Step(s1,a=9)])

print(repr(t1))
await t1([]) #Note for now we still have to call the pipeline as an awaitable, as of current update a fully synchronous path still needs to be implemented.

```

    default={'s1': (<function s1 at 0x000001D8FB58A160>, ('n', 'a'))}
    SimplePipe(name='Simple 1', steps=[Step(function='s1', args=[], kwargs={'a': 1}), Step(function='s1', args=[], kwargs={'a': 2}), Step(function='s1', args=[], kwargs={'a': 3})])
    [1]
    [1, 2]
    [1, 2, 3]



While this looks like a generic Chaining/Pipeline library, we can see one major difference. Functions are registered in the KV store `_functions` of the Pipeline class definition. `Step` objects only hold this key and default arguments for the callable, making the execution graph independent of the functions it uses. You may suspect that this enables two especially modular features:
1. The same steps/graph can be easily swapped into a different Pipeline definition. Maybe you are running a different environment where the implementation details are different, or perhaps you define classes for different library utilities or LLMs e.g. OpenAI modules vs Anthropic.
2. `_functions` is a private attribute, Step keys are strings and default args are assumed to be serializable. As Pipeline and Step are pydantic models, the entire Pipeline object with included steps can be model serialized (see below). So Pipelines function as a network compatible, implementation agnostic procedure graph.

## Definition
To define, first inherit the Pipeline class, then define `functions` using `pipe_funcdict` at the class level. Additionally, functions or coroutines must all have different `__qualname__` meaning that like function names imported from different modules will have conflicts. One remedy is to define static methods, or we can override the keys and then Step needs to be defined with a string:


```python
class test1:
    @staticmethod
    def t(i): return i

class test2:
    @staticmethod
    def t(i): return i
    

class Simple(Pipeline):
    _functions = pipe_funcdict(test1.t, test2.t)

print(Simple._functions)
print('-')
#or like
t= lambda i: i
t1 = lambda i:t(i)
#t.__qualname__='t'
#t1.__qualname__='t1'

class Simple2(Pipeline):
    _functions = pipe_funcdict(('t',t), ('t1',t1))
    #if we don't override qualname then we need to override the name our Pipeline sees.

print(Simple2._functions)
print('-')

#If we don't override qualname then our steps need to be created with our string reference instead of the incorrect qualname.
Step('t'),Step(t) #see incorrect form

```

    default={'test1.t': (<function test1.t at 0x000001754980A480>, ('i',)), 'test2.t': (<function test2.t at 0x000001754980A520>, ('i',))}
    -
    default={'t': (<function <lambda> at 0x000001754980A700>, ('i',)), 't1': (<function <lambda> at 0x000001754980A660>, ('i',))}
    -

    (Step(function='t', args=[], kwargs={}),
     Step(function='<lambda>', args=[], kwargs={}))



### Runtime Mechanics
Next instantiate a pipeline instance and build the procedure of your choice into it. The hierarchy is:
1. Named variables (`kwargs`) in `Step` override named variables in the callables.
2. Named variables with matching names in the `Pipeline` entry override in both the `Step` definition and the callable. This was chosen as there is otherwise no simple way to modify step variables for a single execution, using different named arguments in each function allows for per-call argument modification.
3. Positional arguments in (`args`) in a step will be appended to existing args, args will overflow into kwargs when there are too many. This will override both first call named variables and step named variables.

These behaviors and more are demonstrated here:


```python
async def test1(p,p1='p1',p2='p2',sleep=.5,_st=0.):
    await aio.sleep(sleep)
    et=tm.perf_counter()
    df=et-p
    print(f'Test 1 time offset: {df:.5f}, sleep: {sleep}, p1: {p1}, p2: {p2}, cumulative time: {et-_st:.5f}')
    return et

async def test2(p,p1='p1',p2='p2',sleep=.71,_st=0.):
    await aio.sleep(sleep)
    et=tm.perf_counter()
    df=et-p
    print(f'Test 2 time offset: {df:.5f}, sleep: {sleep}, p1: {p1}, p2: {p2}, cumulative time: {et-_st:.5f}')
    return et#,df

async def test3(p,kwt=None,p3='p1',p2='p2',sleep=.5,_st=0.):
    await aio.sleep(sleep)
    et=tm.perf_counter()
    df=et-p
    print(f'Test 3 time offset: {df:.5f}, sleep: {sleep}, p3: {p3}, p2: {p2}, cumulative time: {et-_st:.5f}, keyword test: {str(kwt)[:10]}')
    return et

def test4(p,p1='p1',p2='p2',sleep=.5,_st=0.):
    tm.sleep(sleep)
    et=tm.perf_counter()
    df=et-p
    print(f'Test 4 time offset: {df:.5f}, sleep: {sleep}, p1: {p1}, p2: {p2}, cumulative time: {et-_st:.5f}')
    return et
    

class TestPipeline(Pipeline):
    _functions: dict[str, tuple[Callable,tuple]] =pipe_funcdict(test1, test2, test3, test4)
    

testp1=TestPipeline(name='Test 1')|Step(test1,)|Step(test2,)|Step(test3,3,sleep=.4,kwt='sb int')|Step(test1,p1='2 last')|Step(test4)
_st=tm.perf_counter()
await aio.gather(testp1(_st,_st=_st),testp1(_st,p1='[not p3]',p2='[Alt time]',kwt='sb int',sleep=.6,_st=_st))
```

    Test 1 time offset: 0.50116, sleep: 0.5, p1: p1, p2: p2, cumulative time: 0.50116
    Test 1 time offset: 0.60182, sleep: 0.6, p1: [not p3], p2: [Alt time], cumulative time: 0.60182
    Test 2 time offset: 0.60014, sleep: 0.6, p1: [not p3], p2: [Alt time], cumulative time: 1.20196
    Test 2 time offset: 0.70092, sleep: 0.71, p1: p1, p2: p2, cumulative time: 1.20208
    Test 3 time offset: 0.40101, sleep: 0.4, p3: p1, p2: p2, cumulative time: 1.60309, keyword test: 3
    Test 3 time offset: 0.61199, sleep: 0.6, p3: p1, p2: [Alt time], cumulative time: 1.81395, keyword test: 3
    Test 1 time offset: 0.49204, sleep: 0.5, p1: 2 last, p2: p2, cumulative time: 2.09512
    Test 4 time offset: 0.50075, sleep: 0.5, p1: p1, p2: p2, cumulative time: 2.59587
    Test 1 time offset: 0.78212, sleep: 0.6, p1: [not p3], p2: [Alt time], cumulative time: 2.59607
    Test 4 time offset: 0.60055, sleep: 0.6, p1: [not p3], p2: [Alt time], cumulative time: 3.19661

    [642549.4583666, 642550.0591074]



For rule #1, see step definition test3 and the second to last step. Rule #2 is seen by the Alt time run. #3 is seen in test3, and additionally test4 demos sync function execution. Note that the second to last printout is delayed significantly longer than it's sleep time of 0.6, this is due to the first concurrent run forcefully freezing the event loop with synchronous sleep.  

Unlike LangChain or LangGraph, pipelines can support both sync and async functions. Therefore running multiple Pipelines concurrently, or launching them as futures, can provide huge performance gains. This could support single threaded servers and benefit from packages like `uvloop` and `winloop`. But as seen in the example synchronous functions should have neglible completion latency, otherwise launching it in a separate thread.

## Storing Pipelines

As pipelines are pydantic models they can be serialized, stored, and loaded from a central repository for the class. No information about the used functions besides their name are stored, and they should still run even if you add or change step kwargs:


```python
#Store
d1=t1.model_dump_json(indent=2)
d2=t2.model_dump_json(indent=2)
d3=t3.model_dump_json(indent=2)
print(d3)
#This can now be written to a json file. Or
#model_dump() and adding to a dict value, can then be drawn to one large json file.
r3=SimplePipe.model_validate_json(d3)
print(repr(r3))
await r3([])

```

    {
      "name": "Simple 3",
      "steps": [
        {
          "function": "s1",
          "args": [],
          "kwargs": {
            "a": 7
          }
        },
        {
          "function": "s1",
          "args": [],
          "kwargs": {
            "a": 8
          }
        },
        {
          "function": "s1",
          "args": [],
          "kwargs": {
            "a": 9
          }
        }
      ]
    }
    SimplePipe(name='Simple 3', steps=[Step(function='s1', args=[], kwargs={'a': 7}), Step(function='s1', args=[], kwargs={'a': 8}), Step(function='s1', args=[], kwargs={'a': 9})])
    [7]
    [7, 8]
    [7, 8, 9]


Potential applications include software, hardware, or domain specific procedure implementations. Safe procedural code generation. Network orchestration for tasks and recourses.

## Advanced

This demo replicates a DAG without needing a proper API. Pre-LangGraph when LangChain was still in early development, I used this method. It managed to be concise and graph-like. The method was to cache the output of certain callables over execution of the procedure that are expensive to regenerate or are non-deterministic. This implicitly creates fan-out dependency edges, fan-in is possible because all callables and awaitables are dispatched at the outer scope of step arguments. For example:

```python
import random as rng
import math as mt
from pipeline import Cache

rcall=0

graph=Cache()
cache= graph.cache()

@cache
async def rand01(n=None):
    global rcall
    rcall+=1
    rd=rng.random()
    print('Random Call:',rcall)
    #to demonstrate that the cache will save the future, and not the result, allowing an arbitrary # of dependencies to wait on it's completion
    #and process as soon as possible after.
    await aio.sleep(.5)
    return rd

def rpr(n,o1=None):
    print('Print:', o1,f'{n:.5f}',f', Second Remainder: {-(mt.floor(et:=tm.perf_counter())-et):.5f}')
    return n

def pp(a,b):
    return a + b
    
    
class GraphDemo(Pipeline):
    _functions: dict[str, tuple[Callable,tuple]] =pipe_funcdict(rand01, rpr, pp)
    
#Can be defined in various ways

rs=Step(rand01)
rg=GraphDemo(name='Random Gen')|rs #Call this node 1, our random recourse
#graph.clear()

#these will print at nearly the same time, and same random value.
ot=await rg(n=None,o1=1)
rpr(ot,1)
ot=await rg(n=None,o1=2) #cache uses inputs, not inputs or default same as lru_cache
rpr(ot,2)

gpn=GraphDemo(name='Random and Print Node')|rs|Step(rpr)
#we have more than a few ways to represent:
ga1=GraphDemo(name='Add Node')|Step(pp)
#ga1=GraphDemo(name='Graph Add 1')|Step(pp,a=gpn(),b=gpn())
#ga1=GraphDemo(name='Graph Add 1')|Step(pp,a=gpn,b=gpn)
ga1w=lambda r1=None, r2=None:ga1(a=gpn(n=r1,o1=1),b=gpn(n=r2,o1=2)) #the fan out print nodes are fan-in through the add. Four nodes total.
ts=tm.perf_counter()
await ga1w()
print(f'Full time: {tm.perf_counter()-ts:.5f}') #demo that there is no wait
graph.clear() #clear the "graph nodes" to try again.

ts=tm.perf_counter()
await ga1w()
print(f'Full time: {tm.perf_counter()-ts:.5f}') #There is delay
graph.clear() 

ts=tm.perf_counter()
await ga1w(r1=1,r2=2)
print(f'Full time: {tm.perf_counter()-ts:.5f}')
#Now there are two implicitly random sources, 5 nodes total, but because they use concurrent recourses, the entire graph is still only 0.5 seconds delayed.
```

    Random Call: 1
    Print: 1 0.60939 , Second Remainder: 0.21638
    Print: 2 0.60939 , Second Remainder: 0.21653
    Print: 1 0.60939 , Second Remainder: 0.21682
    Print: 2 0.60939 , Second Remainder: 0.21685
    Full time: 0.00021
    Random Call: 2
    Print: 1 0.71788 , Second Remainder: 0.71814
    Print: 2 0.71788 , Second Remainder: 0.71824
    Full time: 0.50144
    Random Call: 3
    Random Call: 4
    Print: 1 0.40423 , Second Remainder: 0.21820
    Print: 2 0.66231 , Second Remainder: 0.21829
    Full time: 0.49997
    

We see the DAG is functional. It can also serialize Pipelines that are arguments of a Step class, though it may have it's limits.


```python
#To serialize and deserialize:
ga1=GraphDemo(name='Graph Add 1')|Step(pp,a=GraphDemo().step(rand01,n=1).step(rpr),b=GraphDemo().step(rand01,n=2).step(rpr))
gd=ga1.model_dump_json(indent=2)
print(gd)
import json #need to load in through json.loads, because lack of model_validate_json implementation at this point.
gan=GraphDemo.model_validate(json.loads(gd))
print(repr(gan))
'plus',await gan()
```

    {
      "name": "Graph Add 1",
      "steps": [
        {
          "function": "pp",
          "args": [],
          "kwargs": {
            "a": {
              "name": "NA",
              "steps": [
                {
                  "function": "rand01",
                  "args": [],
                  "kwargs": {
                    "n": 1
                  }
                },
                {
                  "function": "rpr",
                  "args": [],
                  "kwargs": {}
                }
              ]
            },
            "b": {
              "name": "NA",
              "steps": [
                {
                  "function": "rand01",
                  "args": [],
                  "kwargs": {
                    "n": 2
                  }
                },
                {
                  "function": "rpr",
                  "args": [],
                  "kwargs": {}
                }
              ]
            }
          }
        }
      ]
    }
    GraphDemo(name='Graph Add 1', steps=[Step(function='pp', args=[], kwargs={'a': GraphDemo(name='NA', steps=[Step(function='rand01', args=[], kwargs={'n': 1}), Step(function='rpr', args=[], kwargs={})]), 'b': GraphDemo(name='NA', steps=[Step(function='rand01', args=[], kwargs={'n': 2}), Step(function='rpr', args=[], kwargs={})])})])
    Print: None 0.40423 , Second Remainder: 0.51382
    Print: None 0.66231 , Second Remainder: 0.51386

    ('plus', 1.066538244471614)




```python
#system style prompt, AGENTS.md, etc:
c_style1= """You are a math prodigy directly trained by Terence Tao."""

c_style2= """You are a highly regarded physicist with decades long history of contributions."""

c_style3= """You are the sum of all insights and talent that appeared on Curt Jaimungal's podcast."""

c_style4= """You are Edward Witten."""

def fretrieval1(corpus): return f"""Comprehensively distill this corpus into research paths that have the most potential to prove Yang-Mills existence.

Corpus:
{corpus}"""

def fquery1(ri):return f"Prove the Yang-Mills existence theorem. Show all your work, even if you hit a dead end. You may utilize this research:\n{ri}"

def fquery2(r1,r2,r3,ri):return f"""Utilize your colleagues research directions and basis of work to prove Yang-Mills existence. Go as far as you reasonably can.

Colleague 1:
{r1}

Colleague 2:
{r2}

Colleague 3:
{r3}

Other research info:
{ri}"""

#DAG Procedure: Solve Yang-Mills.
#Not, unlike as seen in the DAG every colleague call contains three nodes as a chain:
# user prompt -> chat message -> query LLM

#Colleague system prompts:
c_style1,c_style2,c_style3,c_style4
#f-strings:
fretrieval1,fquery1,fquery2

from pipeline import Pipeline, Cache
#Cache used to replicate DAG
cache=Cache()
cwrap=cache.cache()
#Log parameters
llm_callct=0
corpus_callct=0
def reset_callct():
    global llm_callct,corpus_callct
    llm_callct=corpus_callct=0

#Begin example.
@cwrap
def build_llmquery(user="",system="",_n=1):
    """When system, user, and _n are all the same as a previous call. `init_systemuser` will return the chat-like object that was stored in the cache.
    
    To achieve caching of the cllm_response, this function does need to be cached as cwrap will use the object id as fallback for objects without a __hash__, like lists.
    """
    return [{"role": "system", "content": system},
            {"role": "user", "content": user},]


async def llm_response(chat, model: str = "gpt") -> str: 
    """An over simplified LLM response that sends back the text only."""
    global llm_callct
    llm_callct+=1
    return 'Response to YM solution!'

cllm_response=cwrap(llm_response) #cached version
cllm_response.__qualname__='cllm_response' #needed for GraphLLM to see the separate definition. or we can manually override.

@cwrap #can cache this if it's expensive, however it's not necessary to achieve caching of llm_response, cwrap is to demo total # of calls
def load_corpus(resource_path: str = "") -> str: 
    global corpus_callct
    corpus_callct+=1
    return "Research on YM existence!"


class GraphLLM(Pipeline):
    _functions: dict[str, tuple[Callable,tuple]] =pipe_funcdict(load_corpus, build_llmquery, cllm_response, llm_response, fretrieval1, fquery1, fquery2)
  
make_rinfo=(GraphLLM(name='Make Research Info')
            |Step(load_corpus,recourse_path='Some Path.')
            |Step(fretrieval1) #f-strings have their own internal cache and hash if too large to be cached.
            |Step(build_llmquery, system=c_style3)
            |Step(cllm_response)
            ) #Corpus node.
mk_colleague=lambda s_style:(
        make_rinfo.pipe_copy(name='First Solve')
        |Step(fquery1)
        |Step(build_llmquery,system=s_style)
        |Step(llm_response)
        ) #Colleague node and corpus edge.
#Prove YM node and edges.
solve_c1,solve_c2,solve_c3=mk_colleague(c_style1),mk_colleague(c_style2),mk_colleague(c_style3)

proveym_graph=(GraphLLM(name='Prove Yang Mills')
               |Step(fquery2,r1=solve_c1,r2=solve_c2,r3=solve_c3,ri=make_rinfo)#or,solve_c1,solve_c2,solve_c3,make_rinfo) #or nothing
               |Step(build_llmquery, system=c_style4)
               |Step(llm_response)
               )

await proveym_graph()
print(f'Corpus Calls: {corpus_callct}') #Should be 1
print(f'LLM Calls: {llm_callct}') #Should be 5
print('-')
reset_callct()
#Now because the recourse path hasn't changed, subtract 1 function calls.
await proveym_graph()
print(f'Corpus Calls: {corpus_callct}') #Should be 0
print(f'LLM Calls: {llm_callct}') #Should be 4
print('-')
reset_callct()
cache.clear()
#We cleared the cache so return to full # of graph calls.
await proveym_graph()
print(f'Corpus Calls: {corpus_callct}')
print(f'LLM Calls: {llm_callct}')
print('-')
#Last if we commented out #@cwrap we'd get:
#Corpus Calls: 4
#LLM Calls: 8

#also await proveym_graph(solve_c1,solve_c2,solve_c3,make_rinfo,system=s_style4) #works
#As a production template we can implement load_corpus to take a recourse path from e.g. a task queue.
#We then have an endpoint function that routes to or generates the correct pipeline, and launches it as a future
#where the last step is a POST to the correct address, or whichever needed result.
#resource, recourse 
#If necessary we can also alter the initial path from top-scope like so:
dp='Different Path.'
mkc3=lambda recourse_path:(
    mk_colleague(s)(resource_path=recourse_path) for s in (c_style1, c_style2, c_style3))
#this will produce awaitables so mkc3 has to be called like this at each graph entry.
await proveym_graph(*mkc3(dp), make_rinfo(recourse_path=dp), system=c_style4)
```

    Corpus Calls: 1
    LLM Calls: 5
    -
    Corpus Calls: 0
    LLM Calls: 4
    -
    Corpus Calls: 1
    LLM Calls: 5
    -

    'Response to YM solution!'

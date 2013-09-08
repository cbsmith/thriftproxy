"""
Usage:
    thriftproxy.py [--hostname=HOSTNAME] [--verbose] [--return-values] [--buffered] [--no-trace] <ThriftType.Client> <listen_port> <client_port>

Options:
    --hostname=HOSTNAME   client host [default: 127.0.0.1]
    --no-trace            don't print out method calls
    --return-values       print out return values
    --buffered            use buffered transport instead of framed
    --verbose             print out introspection details
"""

from contextlib import contextmanager
from docopt import docopt
from itertools import chain, ifilter
import re
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer
from thrift.transport import TSocket, TTransport

MAGIC_METHOD_RE = re.compile(R'^__.+?__$')
DEBUGGING = True
verbose = False
return_values = False
tracing = True

def get_interface(client):
    """Get the Thrift iface object associated with a client.
    client -- the client type being inspected"""

    #make sure we get the class
    class_obj = getattr(client, '__class__', client)
    assert not hasattr(class_obj, '__class__')
    #just in case we were passed the interface instead of the client
    if class_obj.__name__ == 'Iface':
        return client

    return next(ifilter(lambda x: x.__name__ == 'Iface', class_obj.__bases__))

def get_service_methods(client):
    """Generate a list of all the methods in a services interface, paired with their argument specification.
    client -- the client type being inspected"""

    interface = get_interface(client)
    base_module_name = interface.__module__
    client_class = getattr(client, '__class__', client)
    for x in dir(interface):
        if not MAGIC_METHOD_RE.match(x):
            args_name = x + '_args'
            base_module = __import__(base_module_name, globals(), locals(), [args_name])
            yield x, getattr(base_module, args_name).thrift_spec

def proxy_wrapper(method, args_thrift_spec):
    """Make a proxy wrapper for a particular method.
    method -- the bound method being wrapped
    args_thrift_spec -- the thrift specification of the arguments to the function"""

    def wrapper(client, *varargs, **kwargs):
        global return_values
        global tracing
        if tracing:
            varargs_gen = ('='.join((args_thrift_spec[1 + x][2], repr(varargs[x]))) for x in xrange(len(varargs)))
            kwargs_gen = ('='.join((key, repr(value),) for (key, value) in kwargs.iteritems()))
            print '%s(%s)' % (method.__name__, ', '.join(chain(varargs_gen, kwargs_gen))),
            if not return_values:
                print
        result = method(*varargs, **kwargs)
        if tracing and return_values:
            print ' -> %r' % result
        return result
    wrapper.__doc__ = 'proxy for ' + method.__name__ + method.__doc__
    return wrapper

def make_proxy_methods(client):
    """Generate a set of methods for a proxy type based on a client specification.
    Results are in method_name: method pairs for easy use with Python's type builtin.
    client -- the client type needing methods"""

    for method_name, thrift_args_spec in get_service_methods(client):
        method = getattr(client, method_name)
        proxy_method = proxy_wrapper(method, thrift_args_spec)
        yield (method_name, proxy_method)

def make_proxy_type(client):
    """Make a proxy type for a client.
    client -- the client type being proxied"""

    method_dict = dict(make_proxy_methods(client))
    return type(client.__module__ + '.Proxy', (object,), method_dict)

def make_proxy(client):
    """Make a proxy object for a client.
    client -- the client to wrap with the proxy
    """

    return make_proxy_type(client)()

@contextmanager
def make_client(client_interface, port, hostname='localhost', buffered=False):
    transport = TSocket.TSocket(hostname, port)
    transport = TTransport.TBufferedTransport(transport) if buffered else TTransport.TFramedTransport(transport)
    try:
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        client = client_interface(protocol)
        transport.open()
        yield client
    finally:
        transport.close()

def main():
    global verbose
    global return_values
    global tracing
    global DEBUGGING
    arguments = docopt(__doc__)
    if DEBUGGING:
        print arguments
    verbose = arguments['--verbose']
    return_values = arguments['--return-values']
    buffered = arguments['--buffered']
    tracing = not arguments['--no-trace']
    if verbose and buffered:
        print 'Using buffered transport'
    client_type = arguments['<ThriftType.Client>']

    base_module_name = client_type[:-7] if client_type.endswith('.Client') else client_type
    base_module = __import__(base_module_name, globals(), locals(), ['Client', 'Processor'])
    client_type = base_module.Client
    processor_type = base_module.Processor
    with make_client(client_type, int(arguments['<client_port>']), arguments['--hostname'], buffered=buffered) as client:
        proxy = make_proxy(client)
        if verbose:
            for (attrname, spec) in get_service_methods(client):
                print attrname, getattr(proxy, attrname).__doc__
        processor = processor_type(proxy)
        transport = TSocket.TServerSocket(port=int(arguments['<listen_port>']))
        tfactory = TTransport.TBufferedTransportFactory() if buffered else TTransport.TFramedTransportFactory()
        pfactory = TBinaryProtocol.TBinaryProtocolFactory()

        server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)
        server.serve()

if __name__ == '__main__':
    main()

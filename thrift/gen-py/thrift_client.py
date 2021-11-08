class ThriftClient:
    from thrift.transport import TSocket, TTransport
    from thrift.protocol import TBinaryProtocol
    from hbase.ttypes import TThriftServerType
    from hbase.ttypes import AlreadyExists, IOError
    from hbase.Hbase import Client, ColumnDescriptor, Mutation

    def __init__(self, hostname, port, is_framed_transport):

        self.host = hostname
        self.port = port
        self.is_framed_transport = is_framed_transport

        from thrift.transport import TSocket, TTransport
        from thrift.protocol import TBinaryProtocol
        from hbase.Hbase import Client
        # Make socket
        socket = TSocket.TSocket(self.host, self.port)

        # Make transport
        if self.is_framed_transport:
            transport = TTransport.TFramedTransport(socket)
        else:
            transport = TTransport.TBufferedTransport(socket)

        # Wrap in a protocol
        protocol = TBinaryProtocol.TBinaryProtocol(transport)

        # Create a client to use the protocol encoder
        self.client = Client(protocol)
        self.transport = transport

    def connect(self):
        self.transport.open()

    def close(self):
        self.transport.close()

    def create_table(self, table, columns):
        from hbase.ttypes import  AlreadyExists
        should_create_table = True
        try:
            for ex_table in self.client.getTableNames():
                if ex_table == table:
                    # raise AlreadyExists('table %s already exists' % table.decode())
                    should_create_table = False
            if should_create_table:
                self.client.createTable(table,columns)
        except AlreadyExists as ae:
            print(ae.message)

import struct
from arrow.arrow import Arrow
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement, BoundStatement, BatchStatement
from cassandra.policies import FallthroughRetryPolicy
import logging

_NOT_SET = object()
_logger = logging.getLogger(__name__)


class SimpleClient(object):
    session = None
    instance = None

    PREPARED_STATEMENT_CACHE = {}

    # warn people if they are missing a routing key on a query
    MISSING_ROUTING_KEY_WARNING = False

    @staticmethod
    def _log_success(result):
        pass

    @staticmethod
    def _log_error(exc):
        _logger.error('batch query error: %s, %s' % (
            exc.__class__.__name__, exc.message
        ))

    def connect(self, nodes, keyspace, **options):
        """ Connect to cassandra cluster
        Arguments:
            :nodes: list of nodes to serve as touchpoints
                for connecting to cluster
            :keyspace: Cassandra keyspace to connect to
            :options: options to pass to Cluster initialization
        """
        cluster = Cluster(nodes, **options)
        metadata = cluster.metadata
        self.session = cluster.connect(keyspace)

        # register arrow with the cql driver encoder
        self.session.encoder.mapping.update({
            Arrow: self.session.encoder.cql_encode_datetime
        })

        _logger.info('Connected to cluster: ' + metadata.cluster_name)
        for host in metadata.all_hosts():
            _logger.info('Datacenter: %s; Host: %s; Rack: %s',
                         host.datacenter, host.address, host.rack)
        self.post_connect_handler()

    def close(self):
        self.session.cluster.shutdown()
        self.session.shutdown()
        self.PREPARED_STATEMENT_CACHE = {}
        self.session = None
        self.instance = None
        _logger.info('Connection closed.')

    @classmethod
    def _pack_routing_key(cls, v):
        """ Converts a routing key value to packed binary strings
        suitable for the cassandra driver.
        """
        if isinstance(v, (list, tuple)):
            return [cls._pack_routing_key(x) for x in v]
        if isinstance(v, int):
            if v > 2 ** 32:
                return struct.pack('>q', v)
            return struct.pack('>i', v)
        if isinstance(v, float):
            return struct.pack('>f', v)
        if isinstance(v, bool):
            return struct.pack('?')
        if isinstance(v, str):
            return v
        if isinstance(v, unicode):
            return v.encode('utf-8')

        # catch all here for some types we don't expect.
        return str(v)

    def _create_simple_statement(self, query, routing_key=None, **kwargs):
        statement = SimpleStatement(query, **kwargs)

        # unfortunately, the routing key when passed into the SimpleStatement
        # init function, doesn't properly handle lists.  Thus, we have to
        # pass it through the property
        if routing_key:
            statement.routing_key = routing_key

        return statement

    def _create_statement(self, query, use_prepared=False, routing_key=None,
                          **kwargs):
        if use_prepared:
            # prepared statements should only be generated once on the
            # server and then reused.  If we have not generated
            # a prepared, go ahead and prepare it
            if query not in self.PREPARED_STATEMENT_CACHE:
                self.PREPARED_STATEMENT_CACHE[query] = (
                    self.session.prepare(query)
                )

            prepared = self.PREPARED_STATEMENT_CACHE[query]
            return BoundStatement(prepared, **kwargs)

        if routing_key:
            routing_key = self._pack_routing_key(routing_key)
        elif self.MISSING_ROUTING_KEY_WARNING:
            _logger.warning(
                "The following query is missing a routing key: %s",
                query
            )

        return self._create_simple_statement(query, routing_key=routing_key,
                                             **kwargs)

    def execute(self, query, params=None, timeout=None, use_prepared=False,
                **kwargs):
        """
        See https://datastax.github.io/python-driver/api/cassandra/query.html
        for an explanation of the fetch_size and consistency_level arguments
        :param query:
        :param params:
        :param timeout:
        :param use_prepared:
        :param kwargs:
        :return:
        """
        statement = self._create_statement(query, use_prepared, **kwargs)
        if use_prepared:
            statement = statement.bind(params)
            return self.session.execute(statement, timeout=timeout)

        return self.session.execute(statement, params, timeout=timeout)

    def execute_async(self, query, params=None, use_prepared=False, **kwargs):
        statement = self._create_statement(query,
                                           use_prepared=use_prepared,
                                           **kwargs)
        if use_prepared:
            statement = statement.bind(params)
            return self.session.execute_async(statement)

        return self.session.execute_async(statement, params)

    def execute_no_paging(self, query, params=None):
        return self.session.execute(
            SimpleStatement(query, fetch_size=100000), params
        )

    def get_batch_query(self):
        return BatchStatement()

    def add_batch_query(self, batch, query, params=None):
        batch.add(query, params)

    def execute_batch(self, batch):
        if not isinstance(batch, BatchStatement):
            raise TypeError("Given query not a BatchStatement")
        return self.session.execute(batch)

    def execute_batch_async(self, batch):
        if not isinstance(batch, BatchStatement):
            raise TypeError("Given query not a BatchStatement")
        future = self.session.execute_async(batch)
        future.add_callbacks(
            callback=SimpleClient._log_success,
            errback=SimpleClient._log_error,
        )

    def post_connect_handler(self):
        """ invoked after the connection has been made """
        pass

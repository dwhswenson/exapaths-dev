import collections

class DAG:
    def __init__(self, edges, nodes=None):
        self.edges = edges
        if nodes is None:
            nodes = set()
        else:
            nodes = set(nodes)

        edge_nodes = set.union(*[set(edge) for edge in edges])
        self.nodes = nodes.union(edge_nodes)

    def execution_order(self):
        yield from ExecutingDAG(self)


class ExecutingDAG:
    """Mutable object for working with DAG execution order"""
    CAP = object()

    def __init__(self, dag, sort_callback=None):
        if sort_callback is None:
            sort_callback = lambda x: x
        self.sort_callback = sort_callback
        self.nodes = set(dag.nodes)
        self.edges = set(dag.edges)
        self.capped = set()
        # counter maps each node to the number of edges pointing to that
        # node (number of dependencies); count_to_node maps the number of
        # counts to a set of nodes with that count
        self._counter, self._count_to_node = self._calculate_counts()

    def _calculate_counts(self):
        # this should only be used when creating the DAG or to test
        # consistency
        to_counts = collections.Counter(to_node for _, to_node in self.edges)
        missing_nodes = set(self.nodes) - set(to_counts) - {self.CAP}

        # ensure consistency; all nodes are listed
        for missing in missing_nodes:
            to_counts[missing] = 0

        # generate count_to_node
        count_to_node = collections.defaultdict(set)
        count_to_node[0] = missing_nodes
        for node, count in to_counts.items():
            if node is not self.CAP:
                count_to_node[count].add(node)
        return to_counts, count_to_node

    def _update_counters(self, node, delta):
        """Reusable method to update counters when edge added/removed"""
        old_count = self._counter[node]
        new_count = old_count + delta
        self._counter[node] = new_count
        self._count_to_node[old_count].remove(node)
        self._count_to_node[new_count].add(node)

    @property
    def available_nodes(self):
        return self._count_to_node[0]

    def cap_nodes(self, nodes):
        for node in nodes:
            caps = {(self.CAP, node) for node in nodes}
            self._update_counters(node, delta=+1)


        self.capped |= set(nodes)
        self.edges |= caps

    def uncap_nodes(self, nodes=None):
        if nodes is None:
            nodes = self.capped

        cap_edges = {(self.CAP, node) for node in nodes}
        self.edges -= cap_edges
        self.capped -= set(nodes)
        for node in nodes:
            self._update_counters(node, delta=-1)

    def find_next_node(self):
        return next(iter(self.sort_callback(self.available_nodes)))

    def mark_node_completed(self, node):
        self.nodes.remove(node)
        del self._counter[node]
        self._count_to_node[0].remove(node)
        node_edges = {(n1, n2) for n1, n2 in self.edges if n1 == node}
        self.edges -= node_edges

        to_nodes = {n for _, n in node_edges}
        for to_node in to_nodes:
            self._update_counters(to_node, delta=-1)

    def __iter__(self):
        return self

    def __next__(self):
        node = self.find_next_node()
        self.mark_node_completed(node)
        return node




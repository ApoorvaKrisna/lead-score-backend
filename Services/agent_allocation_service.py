from flask import Blueprint, jsonify, request
import hashlib
import bisect

agent = Blueprint('agentAllocation', __name__)


@agent.route('/agentAllocation', methods=['POST'])
def agent_allocation():
    new = request.json
    
    return jsonify("lead allocated"), 201



class ConsistentHashing:
    def __init__(self, replicas=3):
        self.replicas = replicas  # Number of virtual nodes for each real node ,  replicas=n*k, k=log(n)
        self.ring = []            # Sorted list of hashed values (the hash ring)
        self.nodes = {}           # Dictionary mapping hashed values to nodes

    def _hash(self, key):
        return int(hashlib.md5(key.encode('utf-8')).hexdigest(), 16)

    def add_node(self, node):
        for i in range(self.replicas):
            virtual_node = f"{node}::{i}"
            hashed_value = self._hash(virtual_node)
            self.ring.append(hashed_value)
            self.nodes[hashed_value] = node
        self.ring.sort()  # Keep the ring sorted for binary search

    def add_nodes_from_client(self, cockroach_client):
        """Add all nodes fetched from the CockroachDB client."""
        nodes = cockroach_client.fetch_nodes()
        if isinstance(nodes, list):
            for node in nodes:
                self.add_node(node)
        else:
            print(nodes)  # Print error if fetching nodes failed

    def remove_node(self, node):
        """Remove a node and its virtual nodes from the hash ring."""
        for i in range(self.replicas):
            virtual_node = f"{node}::{i}"
            hashed_value = self._hash(virtual_node)
            if hashed_value in self.nodes:
                self.ring.remove(hashed_value)
                del self.nodes[hashed_value]

    def get_node(self, key):
        """Get the closest node for a given key."""
        if not self.ring:
            return None
        
        hashed_key = self._hash(key)
        idx = bisect.bisect(self.ring, hashed_key)
        if idx == len(self.ring):
            idx = 0
        return self.nodes[self.ring[idx]]


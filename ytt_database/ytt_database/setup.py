# Script for setting up the database

from arango import ArangoClient


EDGE_COLLECTIONS = {
    'vocalist': ('video', 'channel'),
    'upload': ('channel', 'video')
}


def create_node_collection(ytt, node_coll):
    if not ytt.has_vertex_collection(node_coll):
        ytt.create_vertex_collection(node_coll)


def create_edge_collection(ytt, edge_coll, from_coll, to_coll):
    if not ytt.has_edge_definition(edge_coll):
        ytt.create_edge_definition(
            edge_collection=edge_coll,
            from_vertex_collections=[from_coll],
            to_vertex_collections=[to_coll]
        )


def reset_db():
    client = ArangoClient(hosts='http://localhost:8529')

    sys_db = client.db('_system', username='root', password='password')
    if sys_db.has_database('ytt_db'):
        sys_db.delete_database('ytt_db')



def run():
    client = ArangoClient(hosts='http://localhost:8529')

    sys_db = client.db('_system', username='root', password='password')
    if not sys_db.has_database('ytt_db'):
        sys_db.create_database('ytt_db')

    db = client.db('ytt_db', username='root', password='password')

    # Create graph

    if db.has_graph('ytt_network'):
        ytt = db.graph('ytt_network')
    else:
        ytt = db.create_graph('ytt_network')

    # Create the nodes and edges

    for e, vs in EDGE_COLLECTIONS.items():
        for v in vs:
            create_node_collection(ytt, v)
        create_edge_collection(ytt, e, *vs)

    # Create indices
    channel_coll = db.collection('channel')
    channel_coll.add_hash_index(fields=['handle'], unique=True)


if __name__ == '__main__':
    reset_db()
    run()



# # Truncate the collection.
# students.truncate()

# # Insert new documents into the collection.
# students.insert({'name': 'jane', 'age': 19})
# students.insert({'name': 'josh', 'age': 18})
# students.insert({'name': 'jake', 'age': 21})

# # Execute an AQL query. This returns a result cursor.
# cursor = db.aql.execute('FOR doc IN students RETURN doc')

# # Iterate through the cursor to retrieve the documents.
# student_names = [document['name'] for document in cursor]
#!/usr/bin/env python
"""
Example of using FalkorDB in embedded mode.

This example shows how to use FalkorDB without needing a separate server.
The embedded mode automatically starts a Redis+FalkorDB process that runs
locally and communicates via Unix socket.

Requirements:
    pip install falkordb[embedded]

Note: Embedded mode is only available for synchronous code, not asyncio.
"""

from falkordb import FalkorDB


def main():
    print("=== FalkorDB Embedded Example ===\n")
    
    # Create an embedded FalkorDB instance
    # Data will be stored in memory (ephemeral)
    print("1. Creating embedded FalkorDB instance...")
    db = FalkorDB(embedded=True)
    print("   ✓ Embedded FalkorDB started\n")
    
    # Select a graph
    print("2. Selecting a graph...")
    graph = db.select_graph('social')
    print("   ✓ Graph 'social' selected\n")
    
    # Create some nodes and relationships
    print("3. Creating nodes and relationships...")
    graph.query("""
        CREATE 
            (alice:Person {name: 'Alice', age: 30}),
            (bob:Person {name: 'Bob', age: 35}),
            (charlie:Person {name: 'Charlie', age: 28}),
            (alice)-[:KNOWS]->(bob),
            (bob)-[:KNOWS]->(charlie),
            (charlie)-[:KNOWS]->(alice)
    """)
    print("   ✓ Created 3 people and their relationships\n")
    
    # Query the data
    print("4. Querying the data...")
    result = graph.query("MATCH (p:Person) RETURN p.name, p.age ORDER BY p.age")
    print("   People in the graph:")
    for row in result.result_set:
        name, age = row
        print(f"   - {name}, age {age}")
    print()
    
    # Find connections
    print("5. Finding connections...")
    result = graph.query("""
        MATCH (a:Person)-[:KNOWS]->(b:Person)
        RETURN a.name, b.name
    """)
    print("   Relationships:")
    for row in result.result_set:
        person1, person2 = row
        print(f"   - {person1} knows {person2}")
    print()
    
    # List all graphs
    print("6. Listing all graphs...")
    graphs = db.list_graphs()
    print(f"   Graphs in database: {graphs}\n")
    
    # Clean up
    print("7. Cleaning up...")
    graph.delete()
    print("   ✓ Graph deleted")
    print("   ✓ Embedded FalkorDB will shut down when the program exits\n")
    
    print("=== Example Complete ===")


def persistent_example():
    """
    Example showing how to persist data across connections.
    """
    print("\n=== Persistent Embedded Example ===\n")
    
    import tempfile
    import os
    
    # Create a temporary database file
    tmpdir = tempfile.mkdtemp()
    dbfile = os.path.join(tmpdir, "persistent.db")
    
    print(f"1. Creating persistent database at: {dbfile}")
    
    # First connection - create data
    print("2. First connection - creating data...")
    db1 = FalkorDB(embedded=True, dbfilename=dbfile)
    graph1 = db1.select_graph('persistent_graph')
    graph1.query("CREATE (n:Data {value: 'This data persists!'})")
    print("   ✓ Data created\n")
    
    # Close the first connection
    del graph1
    del db1
    print("3. First connection closed\n")
    
    # Second connection - retrieve data
    print("4. Second connection - retrieving data...")
    db2 = FalkorDB(embedded=True, dbfilename=dbfile)
    graph2 = db2.select_graph('persistent_graph')
    result = graph2.query("MATCH (n:Data) RETURN n.value")
    print(f"   Retrieved value: {result.result_set[0][0]}")
    print("   ✓ Data persisted across connections!\n")
    
    # Clean up
    graph2.delete()
    del graph2
    del db2
    
    # Remove the temporary directory
    import shutil
    shutil.rmtree(tmpdir)
    
    print("=== Persistent Example Complete ===")


if __name__ == "__main__":
    try:
        main()
        persistent_example()
    except ImportError as e:
        if "pip install falkordb[embedded]" in str(e):
            print("ERROR: Embedded FalkorDB is not installed.")
            print("Please run: pip install falkordb[embedded]")
        else:
            raise

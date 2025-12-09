import redis

r = redis.Redis(host='localhost', port=6379, decode_responses=True)


def find_missing():


    all_nodes_list = r.lrange("graph:nodes", 0, -1)


    scored_nodes_keys = r.hkeys("pr:ranks:current")
    scored_nodes_set = set(scored_nodes_keys)

    print(f"   Total Nodes: {len(all_nodes_list)}")
    print(f"   Scored Nodes: {len(scored_nodes_set)}")

    missing_nodes = []
    missing_indices = []

    print("   Scanning for missing nodes...")
    for idx, node_id in enumerate(all_nodes_list):
        if node_id not in scored_nodes_set:
            missing_nodes.append(node_id)
            missing_indices.append(idx)


            if len(missing_nodes) < 5:
                print(f" Found Missing: Index={idx}, ID='{node_id}'")

    count = len(missing_nodes)
    print(f"\nTotal Missing: {count}")

    if count > 0:

        first_idx = missing_indices[0]
        last_idx = missing_indices[-1]
        print(f"Missing Range: Index {first_idx} to {last_idx}")


    else:
        print("No missing nodes found.")


if __name__ == "__main__":
    find_missing()
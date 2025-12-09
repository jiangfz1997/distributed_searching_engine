import redis
import os


r = redis.Redis(host='localhost', port=6379, decode_responses=True)


def audit_system():
    print("Auditing Redis State...")
    try:
        all_nodes = r.llen("graph:nodes")
    except Exception:

        all_nodes = 0

    print(f" Total Nodes in Graph: {all_nodes}")

    if all_nodes == 0:
        print("GRAPH  IS EMPTY!")
        return


    current_key = "pr:ranks:current"



    current_count = r.hlen(current_key)
    print(f"   Nodes with Scores ({current_key}): {current_count}")


    print("   Calculating Total Mass (Sampling)...")


    random_key = r.hrandfield("pr:ranks:current", 10, withvalues=True)
    print(f"   Sample Scores: {random_key}")

    dangling_sum = float(r.get("pr:dangling_sum") or 0)
    print(f"   Current Dangling Sum: {dangling_sum}")


if __name__ == "__main__":
    audit_system()
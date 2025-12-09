import redis
import time
import math
import sys,os,csv

REDIS_HOST = "redis"
TASK_BATCH_SIZE = 2000
MAX_ITERATIONS = 100
DAMPING_FACTOR = 0.85
CONVERGENCE_THRESHOLD = 1e-06 # Convergence threshold for PageRank
LOG_FILE = "/app/log/output/pr_convergence.csv"


def verify_integrity(r, target_key, expected_count, round_id):
    # Only for debugging purposes: verify that the target_key hash has expected_count entries
    # Used for checking data loss caused by parallel writes

    actual_count = r.hlen(target_key)

    if actual_count != expected_count:
        missing = expected_count - actual_count
        print(f"\n [CRITICAL ERROR] Integrity Check Failed in Round {round_id}!")
        print(f"   Target Key: {target_key}")
        print(f"   Expected:   {expected_count}")
        print(f"   Actual:     {actual_count}")
        print(f"   Missing:    {missing} nodes (Approx {missing / TASK_BATCH_SIZE:.1f} batches)")
        return False

    return True


def generate_tasks(r, total_nodes):
    # task generation to redis
    if r.exists("queue:pr:tasks"):
        r.delete("queue:pr:tasks")

    pipe = r.pipeline()
    task_count = 0

    total_tasks = math.ceil(total_nodes / TASK_BATCH_SIZE)

    # Batch generation with pipelining
    PIPELINE_CHUNK = 1000

    print(f" Generating {total_tasks} tasks (Batch Size: {TASK_BATCH_SIZE})...")

    for start in range(0, total_nodes, TASK_BATCH_SIZE):
        pipe.rpush("queue:pr:tasks", f"{start},{TASK_BATCH_SIZE}")
        task_count += 1

        if task_count % PIPELINE_CHUNK == 0:
            pipe.execute()
            percent = (task_count / total_tasks) * 100
            print(f"    - Generated {task_count}/{total_tasks} tasks ({percent:.1f}%)", end='\r')

    pipe.execute()
    print(f"Generated {task_count} tasks in total.        ")

    return task_count


def wait_for_tasks(r, total_tasks):
    # wait for all tasks to complete, prevent early stopping resulting in data loss
    print(f"    Waiting for {total_tasks} tasks to complete...", end='', flush=True)

    while True:

        done_count = int(r.get("sys:phase_ack") or 0)

        percent = (done_count / total_tasks) * 100
        print(f"\r    Waiting for {total_tasks} tasks... {percent:.1f}% ({done_count}/{total_tasks})", end='',
              flush=True)

        if done_count >= total_tasks:
            print("")
            return

        time.sleep(0.2)


def run_controller():
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
    print(f" Logging convergence data to {LOG_FILE}...")
    print(f"CONVERGENCE_THRESHOLD = {CONVERGENCE_THRESHOLD}")
    r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)
    cleanup_state(r)

    if not r.exists("sys:node_count"):
        print("Graph not found! Run graph_loader.py first.")
        sys.exit(1)

    total_nodes = int(r.get("sys:node_count"))
    print(f"Controller Started. Nodes: {total_nodes}")
    if r.exists("pr:ranks:current"):
        if not verify_integrity(r, "pr:ranks:current", total_nodes, 0):
            print("Initial state is corrupted. Please reload the graph.")
            sys.exit(1)

    with open(LOG_FILE, mode='w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Round', 'Duration_Seconds', 'Diff_Value'])

    for round_id in range(1, MAX_ITERATIONS + 1):
        print(f"\n=== ROUND {round_id} START ===")
        start_time = time.time()

        # Phase 1: SCATTER PR VALUES
        print(" -> Phase 1: Scatter")

        r.delete("pr:accumulated")
        r.delete("pr:dangling_sum")
        r.set("sys:phase_ack", 0)
        r.set("sys:signal", "SCATTER")

        num_tasks = generate_tasks(r, total_nodes)


        wait_for_tasks(r, num_tasks)


        dangling_sum = float(r.hget("pr:dangling_sum", "total") or 0.0)

        base_value = (1.0 - DAMPING_FACTOR + (DAMPING_FACTOR * dangling_sum)) / total_nodes

        r.set("sys:base_value", base_value)
        print(f"    (Dangling Sum: {dangling_sum:.4f}, Base Value: {base_value:.8f})")

        # Phase 2: COMPUTE PR VALUES
        print(" -> Phase 2: Compute")
        r.set("sys:convergence_diff", 0.0)
        r.delete("pr:ranks:next")
        r.set("sys:phase_ack", 0)
        r.set("sys:signal", "COMPUTE")


        num_tasks = generate_tasks(r, total_nodes)


        wait_for_tasks(r, num_tasks)

        print(" -> Verifying round integrity...")
        is_valid = verify_integrity(r, "pr:ranks:next", total_nodes, round_id)

        if not is_valid:
            print(" STOPPING CONTROLLER due to data loss.")
            print("   Please check Worker logs for Timeout/Connection errors.")


            sys.exit(1)
        else:
            print(" Integrity Check Passed.")

            # Convergence Check
            total_diff = float(r.get("sys:convergence_diff") or 0.0)
            duration = time.time() - start_time
            print(f"  -> Round {round_id} Done. Time: {duration:.2f}s, Diff: {total_diff:.6f}")
            with open(LOG_FILE, mode='a', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([round_id, round(duration, 4), f"{total_diff:.10f}"])
            if total_diff < CONVERGENCE_THRESHOLD:
                print(f"Converged at Round {round_id}! (Diff {total_diff} < {CONVERGENCE_THRESHOLD})")
                break


            print(" -> Swapping current/next...")
            r.delete("pr:ranks:current")
            r.rename("pr:ranks:next", "pr:ranks:current")

            duration = time.time() - start_time
            print(f"Round {round_id} Done in {duration:.2f}s")

    print("\nPageRank Completed.")
    r.set("sys:signal", "SHUTDOWN")


def cleanup_state(r):

    print(" Cleaning runtime state (keeping graph data)...")

    keys_to_delete = [
        "queue:pr:tasks",
        "sys:signal",
        "sys:phase_ack",
        "sys:base_value",
        "sys:convergence_diff",
        "pr:accumulated",
        "pr:dangling_sum",
        "pr:ranks:next"
    ]

    r.delete(*keys_to_delete)



    print("Runtime state cleared. Ready to start.")
if __name__ == "__main__":
    run_controller()
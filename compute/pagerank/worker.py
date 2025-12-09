import redis
import json
import time
import os
import random

# worker used for page rank computation

REDIS_HOST = "redis"
DAMPING_FACTOR = 0.85


def retry_execute(pipe, max_retries=3, backoff=1):

    for attempt in range(max_retries):
        try:
            return pipe.execute()
        except (redis.ConnectionError, redis.TimeoutError) as e:
            if attempt == max_retries - 1:
                print(f" Pipeline failed after {max_retries} attempts: {e}")
                # print(str(e))
                raise e

            # retry after backoff
            sleep_time = backoff * (2 ** attempt)
            print(f"Pipeline write failed ({e}), retrying in {sleep_time}s...")
            time.sleep(sleep_time)
    return None


def run_worker():
    r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)
    worker_pid = os.getpid()
    print(f"Worker {worker_pid} Ready. Waiting for signals...")
    start_delay = random.uniform(0, 2)
    time.sleep(start_delay)
    while True:
        signal = r.get("sys:signal")

        if signal == "SHUTDOWN":
            print("Shutdown signal received.")
            break

        # wait for controller publish task signal
        if signal not in ["SCATTER", "COMPUTE"]:
            time.sleep(0.2)
            continue

        # fetch task from queue
        raw_task = r.lpop("queue:pr:tasks")

        if not raw_task:
            time.sleep(0.1)
            continue

        try:
            # specific task info: starting index and count
            start_idx, count = map(int, raw_task.split(','))
            end_idx = start_idx + count - 1

            # get node ids for this task
            node_ids = r.lrange("graph:nodes", start_idx, end_idx)
            if not node_ids: continue


            if signal == "SCATTER":
                do_scatter(r, node_ids)

            elif signal == "COMPUTE":
                do_compute(r, node_ids)
            r.incr("sys:phase_ack")
        except Exception as e:
            # IF failed, send the task back so no tasks will be lost
            print(f"Error processing task {raw_task}: {e}")

            print(f"Retrying task {raw_task}...")
            r.lpush("queue:pr:tasks", raw_task)

            # useless?
            time.sleep(1)



def do_scatter(r, nodes):
    # Get current scores and out links, then scatter contributions
    print(" -> Phase 1: Scatter Nodes")
    pipe = r.pipeline()



    for node in nodes:
        pipe.hget("pr:ranks:current", node)
        pipe.hget("graph:out_links", node)

    results = pipe.execute()

    write_pipe = r.pipeline()
    dangling_sum_local = 0.0


    for i in range(0, len(results), 2):
        score_str = results[i]
        links_str = results[i + 1]

        current_score = float(score_str) if score_str else 0.0

        if not links_str:
            # TODO:
            # dangling node, score goes to dangling sum
            # remember mention in the report about dangling nodes
            # lead to rank sink
            dangling_sum_local += current_score
        else:
            targets = json.loads(links_str)
            out_degree = len(targets)
            if out_degree > 0:
                contribution = current_score / out_degree
                for target in targets:
                    write_pipe.hincrbyfloat("pr:accumulated", target, contribution)

    if dangling_sum_local > 0:
        write_pipe.hincrbyfloat("pr:dangling_sum", "total", dangling_sum_local)

    retry_execute(write_pipe)
    print(f"Scatter done for nodes. Dangling Sum Local: {dangling_sum_local}")

def do_compute(r, nodes):
    print(" -> Phase 2: Compute Nodes")
    base_val = float(r.get("sys:base_value") or 0.0)

    pipe = r.pipeline()
    for node in nodes:
        pipe.hget("pr:accumulated", node)
        pipe.hget("pr:ranks:current", node)
    results = pipe.execute()

    write_pipe = r.pipeline()
    local_diff_sum = 0.0

    for i, node in enumerate(nodes):
        accum_val = float(results[i * 2] or 0.0)
        old_score = float(results[i * 2 + 1] or 0.0)

        # Add damping factor
        new_score = base_val + (DAMPING_FACTOR * accum_val)

        write_pipe.hset("pr:ranks:next", node, new_score)

        local_diff_sum += abs(new_score - old_score)

    # print(f"Node count for compute: {len(nodes)}")


    retry_execute(write_pipe)

    # set up convergence diff for early stopping
    if local_diff_sum > 0:
        r.incrbyfloat("sys:convergence_diff", local_diff_sum)

    print(f"Compute done for nodes. Local Diff Sum: {local_diff_sum}")

if __name__ == "__main__":
    run_worker()
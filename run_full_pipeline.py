import subprocess
import time
import sys
import os
import urllib.request
import bz2
import shutil

NUM_MAPPERS = 4
NUM_PR_WORKERS = 4

WIKI_DUMP_URL = "https://dumps.wikimedia.org/simplewiki/latest/simplewiki-latest-pages-articles.xml.bz2"
RAW_DATA_DIR = "data/raw"
BZ2_FILENAME = "simplewiki-latest-pages-articles.xml.bz2"
XML_FILENAME = "simplewiki-latest-pages-articles.xml"

TIMEOUT_MAPPER = 1800
TIMEOUT_PR = 1800


def download_data():
    os.makedirs(RAW_DATA_DIR, exist_ok=True)
    bz2_path = os.path.join(RAW_DATA_DIR, BZ2_FILENAME)
    xml_path = os.path.join(RAW_DATA_DIR, XML_FILENAME)

    if os.path.exists(xml_path):
        print(f"    XML file already exists: {xml_path}")
        return

    if not os.path.exists(bz2_path):
        print(f"    Downloading {BZ2_FILENAME} from Wikimedia...")
        print(f"      URL: {WIKI_DUMP_URL}")
        try:
            def progress(count, block_size, total_size):
                percent = int(count * block_size * 100 / total_size)
                print(f"      Downloading... {percent}%", end="\r")

            urllib.request.urlretrieve(WIKI_DUMP_URL, bz2_path, reporthook=progress)
            print("\n      Download complete.")
        except Exception as e:
            print(f"    Download failed: {e}")
            sys.exit(1)

    print(f"    Extracting {BZ2_FILENAME} ... ")
    try:
        with bz2.open(bz2_path, "rb") as f_in, open(xml_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
        print(f"    Extraction complete: {xml_path}")


        os.remove(bz2_path)
    except Exception as e:
        print(f"    Extraction failed: {e}")
        sys.exit(1)


def log(msg):
    print(f"\n [PIPELINE] {msg}")


def run_cmd(args, description, ignore_error=False):
    print(f"   -> Executing: {description}...")
    start_time = time.time()

    try:
        use_shell = True if os.name == 'nt' else False

        if isinstance(args, list):
            cmd_str = " ".join(args)
        else:
            cmd_str = args

        subprocess.run(cmd_str, check=True, shell=use_shell)

    except subprocess.CalledProcessError as e:
        if ignore_error:
            print(f"    Warning: {description} failed (Ignored).")
        else:
            print(f"    Error in step: {description}")
            print(f"   Command: {cmd_str}")
            sys.exit(1)

    duration = time.time() - start_time
    print(f"    Done ({duration:.2f}s).")


def wait_for_service(service_name, check_cmd, timeout=60):
    print(f"  Waiting for {service_name} to be ready...")
    start = time.time()
    while time.time() - start < timeout:
        try:
            subprocess.run(check_cmd, shell=True, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            print(f" {service_name} is ready.")
            return
        except:
            time.sleep(2)
            print(".", end="", flush=True)
    print(f"\n Timeout waiting for {service_name}.")
    sys.exit(1)


def main():
    total_start = time.time()
    print("=" * 60)
    print("    INDUSTRIAL SEARCH ENGINE - AUTOMATION PIPELINE")
    print("=" * 60)


    log("Step 1: Environment Cleanup & Init")
    run_cmd("docker-compose down --remove-orphans", "Stopping old containers")

    log("Step 1.1: Building Images (ensure dependencies)")
    run_cmd("docker-compose build --no-cache", "Building Docker Images")

    run_cmd("docker-compose up -d redis postgres", "Starting Infrastructure")

    wait_for_service("Postgres", "docker-compose exec postgres pg_isready -U admin")


    drop_sql = "DROP TABLE IF EXISTS inverted_index; DROP TABLE IF EXISTS pagerank; DROP TABLE IF EXISTS metadata;"
    run_cmd(f'docker-compose exec postgres psql -U admin -d search_engine -c "{drop_sql}"', "Dropping old tables")

    run_cmd("docker-compose exec redis redis-cli FLUSHALL", "Flushing Redis")

    run_cmd("docker-compose run --rm compute-node python compute/db_utils.py", "Initializing DB Tables (JSONB)")

    log("Step 1.5: Data Preparation")
    download_data()

    log("Step 2: Data Ingestion (XML -> JSONL)")

    run_cmd("docker-compose run --rm compute-node python ingestion/run_ingestion_multi_process.py", "Running Ingestion")


    log("Step 3: Distributed Indexing")

      # run_cmd("docker-compose run --rm compute-node rm -rf /app/data/temp_shuffle/*", "Cleaning temp files")
    cmd = 'docker-compose run --rm compute-node sh -c "rm -rf /app/data/temp_shuffle/*"'

    run_cmd(cmd, "Cleaning temp files inside Docker")

    run_cmd("docker-compose run --rm compute-node python compute/indexing/controller.py --phase all",
            "Publishing Map&Reduce Tasks")

    print(f"    Launching {NUM_MAPPERS} Mappers...")

    for i in range(NUM_MAPPERS):
        subprocess.run("docker-compose run -d compute-node python compute/indexing/mapper.py", shell=True)

    print("    Waiting for Mappers to finish (Monitor via Docker PS)...")
    wait_start = time.time()
    while True:
        res = subprocess.run('docker ps -q --filter "ancestor=search-compute:v1"', shell=True, capture_output=True,
                             text=True)

        res_q = subprocess.run('docker-compose exec redis redis-cli LLEN queue:indexing:mapper', shell=True,
                               capture_output=True, text=True)
        queue_len = int(res_q.stdout.strip())

        res_p = subprocess.run('docker-compose exec redis redis-cli LLEN queue:indexing:mapper:processing', shell=True,
                               capture_output=True, text=True)
        proc_len = int(res_p.stdout.strip())

        if queue_len == 0 and proc_len == 0:
            print("\n    All Map tasks processed.")
            time.sleep(5)
            break

        if time.time() - wait_start > TIMEOUT_MAPPER:
            print("    Mapper Timeout!")
            sys.exit(1)

        print(f"      Remaining Tasks: {queue_len} | Processing: {proc_len}   ", end='\r')
        time.sleep(2)

    print(f"    Launching {NUM_MAPPERS} Reducers in parallel...")

    for i in range(NUM_MAPPERS):

        cmd = "docker-compose run -d compute-node python compute/indexing/reducer.py"
        subprocess.run(cmd, shell=True, check=True)

    print("   ⏳ Waiting for Reducers to finish...")
    wait_start = time.time()

    while True:

        res_q = subprocess.run('docker-compose exec redis redis-cli LLEN queue:indexing:reducer', shell=True,
                               capture_output=True, text=True)
        res_p = subprocess.run('docker-compose exec redis redis-cli LLEN queue:indexing:reducer:processing', shell=True,
                               capture_output=True, text=True)

        try:
            q_len = int(res_q.stdout.strip())
            p_len = int(res_p.stdout.strip())
        except ValueError:
            q_len = 999
            p_len = 999

        if q_len == 0 and p_len == 0:
            print("\n    All Reduce tasks processed.")
            time.sleep(5)
            break

        print(f"      Remaining: {q_len} | Processing: {p_len}   ", end='\r')
        time.sleep(2)


    log("Step 4: PageRank Calculation")

    # extract_edge
    run_cmd("docker-compose run --rm compute-node python compute/pagerank/extract_edges.py", "Extracting Edges")
    # load graph to redis
    run_cmd("docker-compose run --rm compute-node python compute/pagerank/graph_loader.py", "Loading Graph to Redis")

    print(f"    Starting PR Controller + {NUM_PR_WORKERS} Workers...")
    subprocess.run(f"docker-compose up -d --scale pr-worker={NUM_PR_WORKERS}", shell=True)

    print("    Waiting for PageRank convergence...")
    wait_start = time.time()
    while True:
        res = subprocess.run('docker ps -q -f "name=pr-controller"', shell=True, capture_output=True, text=True)
        if not res.stdout.strip():
            print("\n    PageRank Controller finished.")
            break

        if time.time() - wait_start > TIMEOUT_PR:
            print("    PageRank Timeout!")
            sys.exit(1)

        time.sleep(5)
        print("      Calculation in progress...", end='\r')

    run_cmd("docker-compose run --rm compute-node python compute/pagerank/export_pagerank_sql.py",
            "Exporting PR to Postgres")

    run_cmd("docker-compose stop pr-controller pr-worker", "Stopping PR Cluster")


    log("Step 5: Metadata Export")
    run_cmd("docker-compose run --rm compute-node python compute/export_metadata.py",
            "Exporting Text & Length to Postgres")


    log("Step 6: Deploying Search Engine")
    run_cmd("docker-compose up -d backend", "Starting Backend Service")
    run_cmd("docker-compose up -d frontend", "Starting Frontend Service")


    total_time = time.time() - total_start
    print("\n" + "=" * 60)
    print(f" PIPELINE COMPLETED in {total_time / 60:.2f} minutes!")
    print(" ==> Search API: http://localhost:8000/docs")
    print(" ==> Frontend: http://localhost:8501")
    print("=" * 60)


if __name__ == "__main__":
    main()
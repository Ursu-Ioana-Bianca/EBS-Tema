import platform
import random
import concurrent.futures
import threading
import time

subscription_field_percentages = {
    "company": {"presence": 0.8},
    "value": {"presence": 0.8},
    "drop": {"presence": 0.7},
    "variation": {"presence": 0.6}
}

operator_equal = 0.7


def generate_subscription_field_values(field, count):
    field_values = []
    for _ in range(count):
        if field == "company":
            if random.random() < operator_equal:
                operator = "="
            else:
                operator = "!="
            value = random.choice(["Google", "Apple", "Microsoft"])
        else:
            operator = random.choice(["<", "<=", ">", ">="])
            if field == "value":
                value = random.uniform(80.0, 100.0)
            elif field == "drop":
                value = random.uniform(5.0, 15.0)
            elif field == "variation":
                value = random.uniform(0.6, 0.8)
            elif field == "date":
                value = f"{random.randint(1, 28)}.{random.randint(1, 12)}.{random.randint(2020, 2023)}"
        field_values.append(f"{field},{operator},{value} ")
    return field_values

def generate_subscriptions(num_works, num_subscriptions, filename, lock):
    subscriptions = []
    field_counts = {field: max(1, int(num_subscriptions * percentage["presence"])) for field, percentage in
                    subscription_field_percentages.items()}
    number_fields = sum(field_counts.values())
    matrix = [["" for _ in range(number_fields)] for _ in range(num_subscriptions)]

    def populate_matrix(start, end):
        for j in range(start, end):
            i = 0
            while i < num_subscriptions:
                for field, count in field_counts.items():
                    while count > 0:
                        with lock:
                            if i == num_subscriptions:
                                break
                            field_values = generate_subscription_field_values(field, 1)
                            matrix[i][j] = field_values[0]
                            i += 1
                            count -= 1
                            field_counts[field] -= 1
                if i == num_subscriptions:
                    break
                i += 1

    num_threads = num_works
    chunk_size = num_subscriptions // num_threads
    threads = []

    for i in range(num_threads):
        start = i * chunk_size
        end = start + chunk_size if i < num_threads - 1 else num_subscriptions
        thread = threading.Thread(target=populate_matrix, args=(start, end))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    for i in range(num_subscriptions):
        subscription_str = "{"
        for j in range(number_fields):
            if matrix[i][j] != "":
                if matrix[i][j+1] == " ":
                    subscription_str += "(" + matrix[i][j] + ")"
                else:
                    subscription_str += "(" + matrix[i][j] + ");"
        subscription_str += "}"
        subscriptions.append(subscription_str)

    with lock:
        with open(filename, "w") as file:
            for subscription in subscriptions:
                file.write(str(subscription) + '\n')

    return subscriptions


def generate_publications(num_publications, filename, lock):
    def generate_publication(start, end):
        publications_chunk = []
        for _ in range(start, end):
            company = random.choice(["Google", "Apple", "Microsoft"])
            value = random.uniform(80.0, 100.0)
            drop = random.uniform(5.0, 15.0)
            variation = random.uniform(0.6, 0.8)
            date = f"{random.randint(1, 28)}.{random.randint(1, 12)}.{random.randint(2020, 2023)}"
            publication = {"company": company, "value": value, "drop": drop, "variation": variation, "date": date}
            publications_chunk.append(publication)
        return publications_chunk

    publications = []

    num_threads = min(num_publications, 10)  # Setăm numărul maxim de fire de execuție la 10
    chunk_size = num_publications // num_threads

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        # Generate publications concurrently
        future_to_chunk = {executor.submit(generate_publication, i * chunk_size, (i + 1) * chunk_size): i for i in range(num_threads)}

        for future in concurrent.futures.as_completed(future_to_chunk):
            publications.extend(future.result())

    # Write publications to file
    with lock:
        with open(filename, "w") as file:
            for publication in publications:
                file.write(str(publication) + '\n')

    return publications


def run_task(task_func):
    start_time = time.time()
    task_func()
    end_time = time.time()
    return end_time - start_time


# Funcție pentru a obține specificațiile procesorului
def get_processor_specifications():
     return platform.processor()


lock_generate_publications = threading.Lock()
lock_generate_subscriptions = threading.Lock()


def test_performance(num_subscriptions, num_publications, num_works):
    num_trials = 5  # Number of trials for performance evaluation
    total_time_publications = 0
    total_time_subscriptions = 0
    total_time = 0

    for _ in range(num_trials):
        start_time = time.time()
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_works) as executor:
            future_publications = executor.submit(run_task, lambda: generate_publications(num_publications,
                                                                                        "publications.txt", lock_generate_publications))
            future_subscriptions = executor.submit(run_task,
                                                 lambda: generate_subscriptions(num_works, num_subscriptions, "subscriptions.txt", lock_generate_subscriptions
                                                                        ))
            # Așteaptă obținerea rezultatelor
            time_publications = future_publications.result()
            time_subscriptions = future_subscriptions.result()

            total_time_publications += time_publications
            total_time_subscriptions += time_subscriptions


        end_time = time.time()
        total_time += end_time - start_time

    avg_time_publications = total_time_publications / num_trials
    avg_time_subscriptions = total_time_subscriptions / num_trials
    avg_time_total = total_time / num_trials

    print(f"Performance Test - {num_works} Threads:")
    print(f"Avg. Time for generate_publications: {avg_time_publications} seconds")
    print(f"Avg. Time for generate_subscriptions: {avg_time_subscriptions} seconds")
    print(f"Avg. Time: {avg_time_total} seconds")

    try:
        with open("readme.txt", "a", encoding="utf-8") as readme_file:
            readme_file.write("Paralelizare: Fire de execuție (thread-level parallelism)\n")
            readme_file.write(f"Factorul de paralelism - {num_works} Threads:\n")
            readme_file.write("S-au folosit 5 rulari pentru a calcula average time \n")
            readme_file.write(f"Avg. Time for generate_publications: {avg_time_publications} seconds \n")
            readme_file.write(f"Avg. Time for generate_subscriptions: {avg_time_subscriptions} seconds \n")
            readme_file.write(f"Avg. Time: {avg_time_total} seconds \n")
            readme_file.write("\n")
            readme_file.write("Numarul de mesaje generat: 1000 subscriptii si 1000 publicatii \n")
            # Specificațiile procesorului pot fi obținute prin intermediul unei funcții get_processor_specifications()
            readme_file.write("Specificațiile procesorului: {}\n".format(get_processor_specifications()))
            readme_file.write("\n")

    except Exception as e:
        print("A apărut o eroare la scrierea în fișier:", e)

if __name__ == "__main__":
    num_subscriptions = int(1000)
    num_publications = int(1000)
    test_performance(num_subscriptions, num_publications, 1)
    test_performance(num_subscriptions, num_publications, 4)
    test_performance(num_subscriptions, num_publications, 8)

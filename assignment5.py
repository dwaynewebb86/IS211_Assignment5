import csv
import argparse

# Queue class
class Queue:
    def __init__(self):
        self.items = []
    def is_empty(self):
        return self.items == []
    def enqueue(self, item):
        self.items.append(item)
    def dequeue(self):
        return self.items.pop(0)
    def size(self):
        return len(self.items)

# Request class
class Request:
    def __init__(self, arrival_time, file_requested, processing_time):
        self.arrival_time = arrival_time
        self.file_requested = file_requested
        self.processing_time = processing_time

# Server class
class Server:
    def __init__(self):
        self.current_request = None
        self.time_remaining = 0

    def tick(self):
        if self.current_request is not None:
            self.time_remaining -= 1
            if self.time_remaining <= 0:
                self.current_request = None

    def busy(self):
        return self.current_request is not None

    def start_next(self, new_request):
        self.current_request = new_request
        self.time_remaining = new_request.processing_time

# Function to load requests from a csv
def load_requests(filename):
    requests = []
    with open(filename, newline='') as f:
        reader = csv.reader(f)
        for row in reader:
            arrival_time = int(row[0].strip())
            file_requested = row[1].strip()
            processing_time = int(row[2].strip())
            requests.append(Request(arrival_time, file_requested, processing_time))
    return requests

# Sim one server
def simulateOneServer(filename):
    requests = load_requests(filename)
    server = Server()
    request_queue = Queue()
    waiting_times = []

    max_time = max(r.arrival_time for r in requests) + 10

    for current_second in range(max_time):
        for request in requests:
            if request.arrival_time == current_second:
                request_queue.enqueue(request)

        
        if not server.busy() and not request_queue.is_empty():
            next_request = request_queue.dequeue()
            waiting_times.append(current_second - next_request.arrival_time)
            server.start_next(next_request)

        server.tick()

    average_wait = sum(waiting_times) / len(waiting_times) if waiting_times else 0
    print(f"Average Wait Time: {average_wait:.2f} seconds")
    return average_wait

# Sim many servers
def simulateManyServers(filename, num_servers):
    requests = load_requests(filename)
    servers = [Server() for _ in range(num_servers)]
    queues = [Queue() for _ in range(num_servers)]
    waiting_times = []
    rr_index = 0 

    max_time = max(r.arrival_time for r in requests) + 10

    for current_second in range(max_time):
        for request in requests:
            if request.arrival_time == current_second:
                queues[rr_index].enqueue(request)
                rr_index = (rr_index + 1) % num_servers

        
        for i, server in enumerate(servers):
            if not server.busy() and not queues[i].is_empty():
                next_request = queues[i].dequeue()
                waiting_times.append(current_second - next_request.arrival_time)
                server.start_next(next_request)
            server.tick()

    average_wait = sum(waiting_times) / len(waiting_times) if waiting_times else 0
    print(f"Average Wait Time with {num_servers} servers: {average_wait:.2f} seconds")
    return average_wait

# main function
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--file', required=True, help='Path to the CSV input file')
    parser.add_argument('--servers', type=int, help='Number of servers (optional)')
    args = parser.parse_args()

    if args.servers:
        simulateManyServers(args.file, args.servers)
    else:
        simulateOneServer(args.file)

main()

# Part 3
# With 1 server, the average wait time was 2505.88 seconds. With 3 servers it
# dropped to 0.39 seconds, and with 21+ servers it reached 0.00 seconds.
# This shows that even a small increase in servers dramatically reduces latency.
# To address this we could use an alternative to round-robin like least connection load balancing which is an algorithm where client requests are 
# distributed to the application server with the least number of active connections at the time the client request is received


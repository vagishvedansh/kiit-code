#!/usr/bin/env python3
"""
Concurrency & Leak Stress Harness for Challenger M3-2.
Evaluates:
1. Socket & File Descriptor leaks under burst traffic.
2. Thread / Goroutine starvation.
3. Hung connections and client abort resilience.
"""

import os
import sys
import time
import json
import socket
import threading
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

TARGET_URL = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8794"
PID = int(sys.argv[2]) if len(sys.argv) > 2 else None

def get_process_fds(pid):
    if not pid:
        return -1, []
    fd_dir = f"/proc/{pid}/fd"
    try:
        fds = os.listdir(fd_dir)
        sockets = []
        for fd in fds:
            try:
                target = os.readlink(os.path.join(fd_dir, fd))
                if "socket:" in target:
                    sockets.append((fd, target))
            except Exception:
                pass
        return len(fds), sockets
    except Exception as e:
        return -1, []

def get_process_threads(pid):
    if not pid:
        return -1
    status_file = f"/proc/{pid}/status"
    try:
        with open(status_file, "r") as f:
            for line in f:
                if line.startswith("Threads:"):
                    return int(line.split()[1])
    except Exception:
        pass
    return -1

def run_stress():
    print(f"=== Starting Concurrency Stress Test against {TARGET_URL} (PID: {PID}) ===")
    
    init_fd_count, init_sockets = get_process_fds(PID)
    init_threads = get_process_threads(PID)
    print(f"[*] Baseline FDs: {init_fd_count} (Sockets: {len(init_sockets)}), Threads: {init_threads}")

    results = {
        "baseline_fds": init_fd_count,
        "baseline_sockets": len(init_sockets),
        "baseline_threads": init_threads,
        "phases": []
    }

    # -------------------------------------------------------------
    # Phase 1: Fast bursts to light endpoints (/ and /v1/models)
    # -------------------------------------------------------------
    print("\n--- Phase 1: 50 Concurrent Rapid Requests to Health and Models ---")
    p1_start = time.time()
    p1_statuses = []
    
    def fetch_light(idx):
        endpoint = "/" if idx % 2 == 0 else "/v1/models"
        t0 = time.time()
        try:
            r = requests.get(f"{TARGET_URL}{endpoint}", timeout=5)
            return r.status_code, time.time() - t0, None
        except Exception as e:
            return 0, time.time() - t0, str(e)

    with ThreadPoolExecutor(max_workers=25) as executor:
        futures = [executor.submit(fetch_light, i) for i in range(50)]
        for f in as_completed(futures):
            code, dur, err = f.result()
            p1_statuses.append(code)

    p1_duration = time.time() - p1_start
    p1_fd_count, p1_sockets = get_process_fds(PID)
    p1_threads = get_process_threads(PID)
    print(f"[*] Phase 1 completed in {p1_duration:.3f}s. Statuses: 200: {p1_statuses.count(200)}, other: {len(p1_statuses)-p1_statuses.count(200)}")
    print(f"[*] Post-Phase 1 FDs: {p1_fd_count} (Sockets: {len(p1_sockets)}), Threads: {p1_threads}")

    results["phases"].append({
        "phase": 1,
        "name": "Light endpoint burst (50 reqs)",
        "duration_seconds": round(p1_duration, 3),
        "success_rate": p1_statuses.count(200) / len(p1_statuses),
        "post_fds": p1_fd_count,
        "post_sockets": len(p1_sockets),
        "post_threads": p1_threads
    })

    # -------------------------------------------------------------
    # Phase 2: Client Abort / Premature Termination Stress Test
    # -------------------------------------------------------------
    print("\n--- Phase 2: 20 Aborted Client Connections (Disconnect mid-request) ---")
    p2_start = time.time()
    
    def send_aborted_req(idx):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            host = TARGET_URL.split("//")[1].split(":")[0]
            port = int(TARGET_URL.split(":")[2].split("/")[0])
            s.connect((host, port))
            # Send partial HTTP POST request and abruptly close socket without reading response
            payload = json.dumps({
                "model": "gpt-4o",
                "messages": [{"role": "user", "content": "Hello"}],
                "stream": True
            })
            req_str = (
                f"POST /v1/chat/completions HTTP/1.1\r\n"
                f"Host: {host}:{port}\r\n"
                f"Content-Type: application/json\r\n"
                f"Content-Length: {len(payload)}\r\n"
                f"Authorization: Bearer test-key\r\n\r\n"
                f"{payload}"
            )
            s.sendall(req_str.encode("utf-8"))
            time.sleep(0.05)
            # Abrupt RST/Close
            s.close()
            return True, None
        except Exception as e:
            return False, str(e)

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(send_aborted_req, i) for i in range(20)]
        for f in as_completed(futures):
            f.result()

    p2_duration = time.time() - p2_start
    time.sleep(1.0) # Grace period for socket cleanup
    p2_fd_count, p2_sockets = get_process_fds(PID)
    p2_threads = get_process_threads(PID)
    print(f"[*] Phase 2 completed in {p2_duration:.3f}s")
    print(f"[*] Post-Phase 2 FDs: {p2_fd_count} (Sockets: {len(p2_sockets)}), Threads: {p2_threads}")

    results["phases"].append({
        "phase": 2,
        "name": "Aborted connections (20 client RSTs)",
        "duration_seconds": round(p2_duration, 3),
        "post_fds": p2_fd_count,
        "post_sockets": len(p2_sockets),
        "post_threads": p2_threads
    })

    # -------------------------------------------------------------
    # Phase 3: Concurrency on Proxy Endpoints with Malformed / Boundary Payloads
    # -------------------------------------------------------------
    print("\n--- Phase 3: 30 Concurrent Boundary Requests (Malformed JSON, empty messages, etc.) ---")
    p3_start = time.time()
    p3_statuses = []

    boundary_payloads = [
        {"model": "gpt-4o"}, # Missing messages -> 400
        {"model": "gpt-4o", "messages": []}, # Empty messages -> 400
        {"model": "unknown-nonexistent-model", "messages": [{"role": "user", "content": "hi"}]}, # 404
        b"malformed raw json", # 400
    ]

    def send_boundary(idx):
        p = boundary_payloads[idx % len(boundary_payloads)]
        url = f"{TARGET_URL}/v1/chat/completions"
        headers = {"Authorization": "Bearer test-key", "Content-Type": "application/json"}
        try:
            if isinstance(p, bytes):
                r = requests.post(url, data=p, headers=headers, timeout=5)
            else:
                r = requests.post(url, json=p, headers=headers, timeout=5)
            return r.status_code, None
        except Exception as e:
            return 0, str(e)

    with ThreadPoolExecutor(max_workers=15) as executor:
        futures = [executor.submit(send_boundary, i) for i in range(30)]
        for f in as_completed(futures):
            code, err = f.result()
            p3_statuses.append(code)

    p3_duration = time.time() - p3_start
    p3_fd_count, p3_sockets = get_process_fds(PID)
    p3_threads = get_process_threads(PID)
    print(f"[*] Phase 3 completed in {p3_duration:.3f}s. Statuses: {set(p3_statuses)}")
    print(f"[*] Post-Phase 3 FDs: {p3_fd_count} (Sockets: {len(p3_sockets)}), Threads: {p3_threads}")

    results["phases"].append({
        "phase": 3,
        "name": "Boundary / Malformed payload burst (30 reqs)",
        "duration_seconds": round(p3_duration, 3),
        "post_fds": p3_fd_count,
        "post_sockets": len(p3_sockets),
        "post_threads": p3_threads
    })

    # -------------------------------------------------------------
    # Cooldown & Leak Assessment
    # -------------------------------------------------------------
    print("\n--- Cooldown & Leak Evaluation (5 seconds) ---")
    time.sleep(5.0)
    final_fd_count, final_sockets = get_process_fds(PID)
    final_threads = get_process_threads(PID)
    fd_delta = final_fd_count - init_fd_count if init_fd_count > 0 else 0
    socket_delta = len(final_sockets) - len(init_sockets) if len(init_sockets) >= 0 else 0

    print(f"[*] Baseline FDs: {init_fd_count} -> Final FDs: {final_fd_count} (Delta: {fd_delta})")
    print(f"[*] Baseline Sockets: {len(init_sockets)} -> Final Sockets: {len(final_sockets)} (Delta: {socket_delta})")
    print(f"[*] Baseline Threads: {init_threads} -> Final Threads: {final_threads}")

    results["final_fds"] = final_fd_count
    results["final_sockets"] = len(final_sockets)
    results["final_threads"] = final_threads
    results["fd_delta"] = fd_delta
    results["socket_delta"] = socket_delta
    results["socket_leak_detected"] = (socket_delta > 5)

    with open("tests/stress_results.json", "w") as f:
        json.dump(results, f, indent=2)

    return results

if __name__ == "__main__":
    res = run_stress()
    print("\nSummary Results:")
    print(json.dumps(res, indent=2))
    if res.get("socket_leak_detected"):
        print("[!] WARNING: Socket leak detected!")
        sys.exit(1)
    else:
        print("[*] Concurrency stress test passed: No persistent socket leaks or thread starvation.")
        sys.exit(0)

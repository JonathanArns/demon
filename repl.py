#!/bin/python

import requests
import sys
import time

query_mode = "weak"

addr = None
addrs_to_try = [
    "localhost:80",
    "localhost:8000",
    "localhost:8080",
]

for item in addrs_to_try:
    try:
        resp = requests.get(f"http://{item}")
        if resp.text == "Hello world!":
            addr = item
            break
    except:
        pass
if addr == None:
    print(f"could not find locally running demon\nconnect with `addr <address>`")
else:
    print(f"found demon at `{addr}`\nswitch to different node with `addr <address>`")

def process_input(line):
    global addr
    global query_mode
    parts = line.split(" ", 1)
    command = parts[0]
    args = parts[1] if len(parts) > 1 else ""
    query = command

    if "exit" == command:
        exit("Goodbye")

    elif "addr" == command:
        try:
            requests.get(f"http://{args}")
            if resp.text == "Hello world!":
                addr = args
                print("connected")
            else:
                print(f"wrong response: {resp.text}")
        except:
            print("could not connect")
        return

    elif "weak" == command or "w" == command:
        query_mode = "weak"
        if args:
            query = args
        else:
            return

    elif "strong" == command or "s" == command:
        query_mode = "strong"
        if args:
            query = args
        else:
            return

    elif "test" == command:
        run_tests()
        return

    if not addr:
        print(f"not connected. connect with `addr <address>`")
    resp = requests.post(f"http://{addr}/{query_mode}", data=query)
    print(resp.text)

def run_tests():
    run_test("fabi's deadlock", ["1+1", "w r1", "1+12", "r1", "s r1", "r1", "1+2", "1-12", "r1", "1=4", "r1", "w 2+1", "r2", "2=4", "s 2=7"])
    run_test("fabi's deadlock 2", ["w 1=1", "s 1=2", "1+2", "r2", "r1", "w 1=1", "r1", "s 1=2"])

def run_test(name, scenario):
    global addr
    print(f"Running test: {name}")
    if not addr:
        print(f"not connected. connect with `addr <address>`")
        return
    for cmd in scenario:
        time.sleep(0.1)
        print(f"> {cmd}")
        process_input(cmd)
    print(f"Completed test: {name}")


while True:
    line = str(input("> ")).strip()
    process_input(line)

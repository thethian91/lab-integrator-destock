import argparse
import pathlib
import socket


def main():
    p = argparse.ArgumentParser(description="Send a file over TCP (e.g., HL7)")
    p.add_argument("--host", required=True)
    p.add_argument("--port", type=int, required=True)
    p.add_argument("--file", required=True)
    args = p.parse_args()

    data = pathlib.Path(args.file).read_bytes()
    with socket.create_connection((args.host, args.port)) as s:
        s.sendall(data)
    print(f"Sent {len(data)} bytes to {args.host}:{args.port}")


if __name__ == "__main__":
    main()

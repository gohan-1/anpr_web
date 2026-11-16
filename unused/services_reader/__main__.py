"""
Start one copy of the reader service.

  python -m services.reader                              # 1 copy, port 8101
  python -m services.reader --port 8102 --shard 2/2      # second of two copies
  python -m services.reader --env staging                # use the staging database

Each copy needs its own port (on one machine) and its own shard number.
"""
import argparse
import os
import socket

import uvicorn


def main():
    ap = argparse.ArgumentParser(description="Reader service")
    ap.add_argument("--host", default="127.0.0.1", help="use 0.0.0.0 inside a container")
    ap.add_argument("--port", type=int, default=8101)
    ap.add_argument("--shard", default="1/1", help="which share of the folders this copy handles, e.g. 2/3")
    ap.add_argument("--env", help="local, staging or production")
    args = ap.parse_args()

    # Settings are passed to the app through environment variables, the same way a container gets them.
    os.environ["READER_SHARD"] = args.shard
    os.environ["READER_PORT"] = str(args.port)
    os.environ.setdefault("READER_INSTANCE", f"{socket.gethostname()}:{args.port}")
    if args.env:
        os.environ["ANPR_ENV"] = args.env
    uvicorn.run("services.reader.app:app", host=args.host, port=args.port, log_level="info")


if __name__ == "__main__":
    main()

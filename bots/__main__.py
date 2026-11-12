"""
Start one bot:

  python -m bots --stage preprocess --name pre-1
  python -m bots --stage reader --name reader-1 --shard 1/3
  python -m bots --stage ocr --name ocr-1 --env staging

Normally the supervisor starts bots for you (one per Bot row); this command is what it runs.
"""
import argparse
import os
import sys
from pathlib import Path


def main():
    from pipeline.stages import BOT_STAGES   # no Django needed for this import
    ap = argparse.ArgumentParser(description="Run one assembly-line bot")
    ap.add_argument("--stage", required=True, choices=BOT_STAGES)
    ap.add_argument("--name", required=True, help="unique bot name, e.g. ocr-2")
    ap.add_argument("--shard", default="1/1", help="reader bots only: which share of the folders, e.g. 2/3")
    ap.add_argument("--env", help="local, staging or production")
    ap.add_argument("--yes", action="store_true", help="skip the staging/production confirmation")
    args = ap.parse_args()
    if args.env:
        os.environ["ANPR_ENV"] = args.env

    import services.common.django_setup  # noqa: F401  (Django + database, after ANPR_ENV is set)
    from core.safety import confirm_environment, require_writable
    require_writable(f"start a {args.stage} bot")
    confirm_environment(f"start a {args.stage} bot", assume_yes=args.yes)

    from bots.runner import ReaderBot, StageBot
    bot = ReaderBot(args.name, args.shard) if args.stage == "reader" else StageBot(args.name, args.stage)
    bot.run()


if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    main()

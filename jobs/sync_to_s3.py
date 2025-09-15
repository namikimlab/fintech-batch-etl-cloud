# sync_to_s3.py
# Minimal: sync a local silver directory to S3 with AWS CLI.

import argparse, os, subprocess, sys

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--local-silver-dir", required=True, help="e.g., ./data/silver")
    ap.add_argument("--s3-dst", required=True, help="e.g., s3://your-bucket/silver")
    ap.add_argument("--only-show-errors", action="store_true", help="Passes --only-show-errors to aws cli")
    ap.add_argument("--delete", action="store_true", help="Deletes files in destination that don't exist locally")
    args = ap.parse_args()

    src = os.path.abspath(args.local_silver_dir)
    dst = args.s3_dst.rstrip("/")

    if not os.path.isdir(src):
        print(f"ERROR: local path not found: {src}", file=sys.stderr)
        sys.exit(2)

    cmd = ["aws", "s3", "sync", src, dst]
    if args.only_show_errors:
        cmd.append("--only-show-errors")
    if args.delete:
        cmd.append("--delete")

    res = subprocess.run(cmd, check=False, capture_output=True, text=True)
    if res.returncode != 0:
        print(res.stderr.strip(), file=sys.stderr)
        sys.exit(res.returncode)
    print("OK: sync completed.")

if __name__ == "__main__":
    main()

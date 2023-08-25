import subprocess
import sys


def check_version_bump() -> None:
    result = subprocess.run(
        ["git", "diff", "--cached", "--name-only"], stdout=subprocess.PIPE, check=False
    )
    changed_files = result.stdout.decode().splitlines()

    if "gemini_python/version.txt" in changed_files:
        print("Version file updated.")
    else:
        print("Error: Version file not updated.")
        sys.exit(1)


if __name__ == "__main__":
    check_version_bump()

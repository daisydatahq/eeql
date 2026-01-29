from __future__ import annotations

import argparse
import importlib.resources as resources
import shutil
import subprocess
from pathlib import Path


def _find_editors() -> list[str]:
    """Return available editor CLIs we can target, in priority order."""
    found = []
    for cmd in ["code", "code-insiders", "cursor"]:
        if shutil.which(cmd):
            found.append(cmd)
    return found


def _vsix_path() -> Path | None:
    try:
        return Path(resources.files("eeql._vscode") / "eeql-lsp-client.vsix")
    except FileNotFoundError:
        return None


def _build_vsix() -> Path:
    root = Path(__file__).resolve().parent.parent.parent
    ext_dir = root / "vscode-extension"
    if not ext_dir.exists():
        raise RuntimeError("vscode-extension/ not found; cannot build vsix.")
    subprocess.run(["npm", "ci"], cwd=ext_dir, check=True)
    subprocess.run(["npm", "run", "package"], cwd=ext_dir, check=True)
    candidates = list(ext_dir.glob("*.vsix"))
    if not candidates:
        raise RuntimeError("vsix package not produced")
    return candidates[0]


def cmd_vscode_install():
    editors = _find_editors()
    if not editors:
        raise RuntimeError(
            "No supported editor CLI found. Install the 'code' (or 'cursor') command in PATH."
        )
    vsix = _vsix_path()
    if not vsix or not vsix.exists():
        vsix = _build_vsix()
    for editor in editors:
        subprocess.run([editor, "--install-extension", str(vsix), "--force"], check=True)
        print(f"Installed EEQL extension into {editor} from {vsix}. Restart the editor.")


def cmd_vscode_uninstall():
    editors = _find_editors()
    if not editors:
        raise RuntimeError(
            "No supported editor CLI found. Install the 'code' (or 'cursor') command in PATH."
        )
    for editor in editors:
        subprocess.run([editor, "--uninstall-extension", "eeql.eeql-lsp-client"], check=True)
        print(f"Uninstalled EEQL extension from {editor}.")


def main(argv=None):
    parser = argparse.ArgumentParser(prog="eeql")
    sub = parser.add_subparsers(dest="cmd")
    vscode = sub.add_parser("vscode")
    vscode_sub = vscode.add_subparsers(dest="vscode_cmd")
    vscode_sub.add_parser("install")
    vscode_sub.add_parser("uninstall")
    args = parser.parse_args(argv)

    if args.cmd == "vscode":
        if args.vscode_cmd == "install":
            cmd_vscode_install()
        elif args.vscode_cmd == "uninstall":
            cmd_vscode_uninstall()
        else:
            parser.print_help()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()

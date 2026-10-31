#!/usr/bin/env bash
# Delete the demo Kind cluster and all of its resources.
set -euo pipefail

CALLER_HOME="${HOME}"
if [ -n "${SUDO_USER:-}" ]; then
  CALLER_HOME="$(getent passwd "$SUDO_USER" 2>/dev/null | cut -d: -f6 || true)"
  if [ -z "$CALLER_HOME" ]; then
    CALLER_HOME="$HOME"
  fi
fi
for tool_dir in "$CALLER_HOME/.local/bin" "/usr/local/bin" "/snap/bin"; do
  if [ -d "$tool_dir" ]; then
    case ":$PATH:" in
      *":$tool_dir:"*) ;;
      *) PATH="$tool_dir:$PATH" ;;
    esac
  fi
done
export PATH

CLUSTER="${KIND_CLUSTER:-gofs-demo}"
if kind get clusters | grep -qx "$CLUSTER"; then
  kind delete cluster --name "$CLUSTER"
else
  echo "Kind cluster $CLUSTER does not exist"
fi

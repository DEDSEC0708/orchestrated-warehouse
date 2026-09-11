#!/usr/bin/env bash
#
# secrets.sh - random value generation for the credential scripts.
#
# Sourced, not executed:  . "${REPO_ROOT}/scripts/lib/secrets.sh"
#
# WHY THIS IS NOT PYTHON
#
#   These scripts run on developer machines, and on Windows that means Git
#   Bash. Requiring a host Python there is a real cost, and it fails in a
#   uniquely confusing way:
#
#     Python was not found; run without arguments to install from the
#     Microsoft Store, or disable this shortcut from Settings > Apps >
#     Advanced app settings > App execution aliases.
#
#   Windows ships an "App Execution Alias" at
#   %LOCALAPPDATA%\Microsoft\WindowsApps\python3.exe - a zero-byte reparse
#   point that exists, sits on PATH, and is marked executable. So the obvious
#   guard passes:
#
#     command -v python3 >/dev/null || die "python3 not found"    # SUCCEEDS
#
#   and the script then dies later, in the middle of its work, on a stub that
#   only knows how to advertise the Store. Existence is not executability, and
#   on Windows that distinction has teeth.
#
#   The fix is not a better probe. It is not needing the interpreter: openssl
#   and awk both ship with Git for Windows, are present on every Linux runner,
#   and are already assumed by the rest of this repository. Nothing here needs
#   a language runtime.
#
# ENTROPY
#   openssl rand draws from the OS CSPRNG. /dev/urandom is the fallback for a
#   host without openssl; it is the same source. There is deliberately no
#   fallback to $RANDOM, date, or process ids - a weak password generated
#   silently is worse than a script that stops.

# ---------------------------------------------------------------------------
# require_entropy_source - fail early and clearly if we cannot generate safely.
# ---------------------------------------------------------------------------
require_entropy_source() {
    if command -v openssl >/dev/null 2>&1 && openssl rand -hex 1 >/dev/null 2>&1; then
        return 0
    fi
    if [ -r /dev/urandom ]; then
        return 0
    fi
    echo "ERROR: no source of cryptographic randomness found." >&2
    echo "       Needs either 'openssl' on PATH or a readable /dev/urandom." >&2
    echo "       Both ship with Git for Windows and with every Linux distro," >&2
    echo "       so this usually means a stripped-down container." >&2
    return 1
}

# ---------------------------------------------------------------------------
# _random_bytes_base64 <byte_count> - raw randomness as standard base64.
# ---------------------------------------------------------------------------
_random_bytes_base64() {
    local count="$1"
    if command -v openssl >/dev/null 2>&1 && openssl rand -hex 1 >/dev/null 2>&1; then
        openssl rand -base64 "${count}"
    else
        # base64 is in coreutils, which Git Bash also provides.
        head -c "${count}" /dev/urandom | base64
    fi
}

# ---------------------------------------------------------------------------
# random_alnum <length> - letters and digits only.
#
# Alphanumeric is a deliberate restriction, not laziness: these values end up
# in the userinfo section of connection URIs, in a .env file, in a YAML value
# and in a shell word. Restricting the alphabet means none of those four ever
# needs quoting or percent-encoding, so there is no layer where an escaping
# mistake can silently produce the wrong password.
#
# Filtering base64 output down to [A-Za-z0-9] keeps every retained character
# uniform over 62 symbols - dropping symbols from a uniform alphabet does not
# bias the ones that remain. 32 characters is about 190 bits.
# ---------------------------------------------------------------------------
random_alnum() {
    local want="${1:-32}"
    local out=""
    # Loop rather than assume one draw survives filtering; base64 loses roughly
    # 5% of its characters to '+', '/' and '='.
    while [ "${#out}" -lt "${want}" ]; do
        out="${out}$(_random_bytes_base64 96 | LC_ALL=C tr -dc 'A-Za-z0-9')"
    done
    printf '%s' "${out:0:${want}}"
}

# ---------------------------------------------------------------------------
# random_hex <byte_count> - hex string, twice the byte count in characters.
# ---------------------------------------------------------------------------
random_hex() {
    local count="${1:-32}"
    if command -v openssl >/dev/null 2>&1 && openssl rand -hex 1 >/dev/null 2>&1; then
        openssl rand -hex "${count}"
    else
        head -c "${count}" /dev/urandom | od -An -tx1 | LC_ALL=C tr -d ' \n'
    fi
}

# ---------------------------------------------------------------------------
# random_fernet_key - exactly what cryptography.fernet.Fernet.generate_key()
# returns: 32 random bytes in URL-safe base64, padding included.
#
#   Fernet.generate_key() == base64.urlsafe_b64encode(os.urandom(32))
#
# so this is the same construction, not an approximation of it. Doing it here
# also removes the old requirement that the host have the `cryptography`
# package installed, which was the other reason generate_env.sh needed Python.
# ---------------------------------------------------------------------------
random_fernet_key() {
    _random_bytes_base64 32 | LC_ALL=C tr -d '\n' | LC_ALL=C tr '+/' '-_'
}

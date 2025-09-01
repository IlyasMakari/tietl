import re
import json
import regex
from typing import Iterable, List, Tuple, Callable, Union
import pandas as pd
import fsspec


def _replace_schemes(line: str) -> Tuple[str, List[Tuple[str, str]]]:
    URL_SCHEMAS = [
    "http://", "https://", "ftp://", "ftps://", "sftp://",
    "ssh://", "smtp://", "imap://", "pop3://", "file://",
    "android://", "chrome://", "edge://", "about://",
    "mailbox://", "oauth://", "moz-extension://",
    "chrome-extension://",
    ]
    replacements = []
    mod = line
    for i, schema in enumerate(URL_SCHEMAS):
        if schema in mod:
            placeholder = f"__SCHEMA_{i}__"
            mod = mod.replace(schema, placeholder)
            replacements.append((placeholder, schema))
    return mod, replacements

def _replace_json_objects(line: str) -> Tuple[str, List[Tuple[str, str]]]:
    """
    Replace valid JSON objects in a string with placeholders.

    Returns:
        modified_line, replacements (list of (placeholder, original_json))
    """

    # Recursive regex to capture nested {...}
    pattern = r"""
    \{              # { character
        (?:         # non-capturing group
            [^{}]   # anything that is not a { or }
            |       # OR
            (?R)    # recurse to match nested {...}
        )*          # repeat
    \}              # } character
    """

    replacements: List[Tuple[str, str]] = []
    mod = line

    matches = list(regex.finditer(pattern, line, flags=regex.VERBOSE))
    for i, m in enumerate(reversed(matches)):
        # reversed so indexes remain valid when replacing substrings
        json_str = m.group(0)
        try:
            parsed = json.loads(json_str)
            if isinstance(parsed, dict) and len(parsed) > 0:
                placeholder = f"__JSON_{len(replacements)}__"
                # replace only this slice
                start, end = m.span()
                mod = mod[:start] + placeholder + mod[end:]
                replacements.append((placeholder, json_str))
        except json.JSONDecodeError:
            continue  # ignore invalid JSON

    # Note: replacements order matters for restore
    replacements.reverse()
    return mod, replacements

def _replace_ports(s: str, delimiter: str) -> Tuple[str, List[Tuple[str, str]]]:
    """
    Replace valid port numbers in a string with placeholders.

    Returns:
        modified_string, replacements (list of (placeholder, original_port))
    """
    replacements: List[Tuple[str, str]] = []
    allowed_after = set([delimiter, "/", "?", "#"])
    allowed_after_pattern = "[" + re.escape("".join(allowed_after)) + "]|$"

    pattern = re.compile(r":([1-9][0-9]{0,4})(?=" + allowed_after_pattern + ")")

    mod = s
    for i, m in enumerate(reversed(list(pattern.finditer(s)))):
        port_str = m.group(1)
        port_val = int(port_str)
        if 1 <= port_val <= 65535:
            ph = f"__PORT_{len(replacements)}__"
            start, end = m.span()
            mod = mod[:start] + ph + mod[end:]
            replacements.append((ph, m.group(0)))

    # Reverse so replacements list matches original order in the string
    replacements.reverse()
    return mod, replacements

def _restore_placeholders(text: str, placeholders: List[Tuple[str, str]]) -> str:
    for ph, orig in placeholders:
        text = text.replace(ph, orig)
    return text


def _email_inference_parse(s: str, d: str, lineno: int) -> Tuple[str, str, str, str]:
    EMAIL_RE = re.compile(r'([a-zA-Z0-9._%+\-]{1,64}@[a-zA-Z0-9.\-]+\.[A-Za-z]{2,})')
    matches = list(EMAIL_RE.finditer(s))
    if not matches:
        return ("bad_format", "", "", "")

    # Take the last match (most likely to be the login, e.g. if host also contains a email-like string)
    m = matches[-1]
    e_start, e_end = m.span()
    email = m.group(1)

    before = s[e_start - 1] if e_start - 1 >= 0 else None
    after  = s[e_end] if e_end < len(s) else None

    # Case 1: email delimited on both sides -> host:email:password
    if before == d and after == d:
        host = s[:e_start - 1]
        login = email
        password = s[e_end + 1:]
        return ("success", host.strip(), login.strip(), password.strip())

    # Case 2: email at beginning and followed by delimiter -> email:password or email:password:host
    if after == d and e_start == 0:
      if s.count(d) == 1: # if email:password
        host = ""
        login = email
        password = s[e_end + 1:]
        return ("success", host, login.strip(), password.strip())
      elif s.count(d) == 2 and s.split(d)[-1].startswith("__SCHEMA_"): # if email:password:host
        parts = s.split(d)
        host = parts[-1]
        login = email
        password = parts[-2]
        return ("success", host.strip(), login.strip(), password.strip())
      else:
        return ("ambiguous", "", "", "")

    return ("bad_format", "", "", "")


def _structural_split_parse(s: str, d: str, lineno: int, placeholders: List[Tuple[str, str]] = None) -> Tuple[bool, str, str, str]:

    if placeholders is None:
          placeholders = []

    idxs = [i for i, ch in enumerate(s) if ch == d] # delimiter positions
    n = len(idxs) # number of delimiters found

    if n == 1:
        left, right = s.split(d, 1)
        if left and right:
          return ("success", "", _restore_placeholders(left.strip(), placeholders), _restore_placeholders(right.strip(), placeholders))
        return ("bad_format", "", "", "")

    if n == 2:
        left = s[:idxs[0]]
        middle = s[idxs[0]+1:idxs[1]]
        right = s[idxs[1]+1:]

        if right.startswith("__SCHEMA_"): # if login:pass:host
            return ("success", _restore_placeholders(right.strip(), placeholders), _restore_placeholders(left.strip(), placeholders), _restore_placeholders(middle.strip(), placeholders))

        # else, assume host:login:pass
        return ("success", _restore_placeholders(left.strip(), placeholders), _restore_placeholders(middle.strip(), placeholders), _restore_placeholders(right.strip(), placeholders))

    return ("ambiguous", "", "", "")


def _parse_with_delimiter(line: str, d: str, lineno: int) -> Tuple[bool, str, str, str]:
    s = line.strip()
    if not s:
        return ("bad_format", "", "", "")

    # --- Strategy 1: email-based inference ---
    s1_status, host, login, password = _email_inference_parse(s, d, lineno)
    if s1_status == "success" and login and password:
        return (s1_status, host, login, password)

    # --- Strategy 2: structural split ---
    s2_status, host, login, password = _structural_split_parse(s, d, lineno)
    if s2_status == "success" and login and password:
        return (s2_status, host, login, password)

    return ((s1_status, s2_status), "", "", "")


def parse_ulp_line(
    line: str,
    lineno: int,
    possible_delimiters: Iterable[str] = (":", "|"),
    on_success: Callable[[str, str, str], None] = None,
    on_error: Callable[[str, int, str], None] = None,
) -> None:
    """
    Parse a single log line.

    Parameters:
    - on_success(host, login, password): called when a line is successfully parsed
    - on_error(log, lineno, line): called on errors
    """
    stripped = line.strip()
    if not stripped:
        return

    mod_line, scheme_replacements = _replace_schemes(stripped)
    mod_line, json_replacements = _replace_json_objects(mod_line)
    replacements = scheme_replacements + json_replacements

    status_log = {}

    for d in possible_delimiters:
        if d not in mod_line:
            continue

        idxs = [i for i, ch in enumerate(mod_line) if ch == d] # delimiter positions
        n = len(idxs) # number of delimiters found
        if n > 2: # if 3 or more delimiter are found it could be caused by a port after the domain or ip address of the host
          secondtolast = idxs[-2] # delimiter that is "probably" after the host if no other conflicting delimiters exist
          left = mod_line[:secondtolast] # part of the string that "probably" contains the host if no other conflicting delimiters exist
          mod_left, port_replacements = _replace_ports(left, d) # replace ports in the host part
          mod_line = mod_left + d + mod_line[secondtolast+1:] # reconstuct the string (left + delimiter + rest)
          replacements += port_replacements

        status, host, login, password = _parse_with_delimiter(mod_line, d, lineno)
        status_log[d] = status
        if status == "success" and login and password:
            if on_success:
                on_success(lineno, line, {
                    "host": _restore_placeholders(host, replacements),
                    "login": _restore_placeholders(login, replacements),
                    "password": _restore_placeholders(password, replacements)
                })
            return

    if on_error:
        on_error(status_log, lineno, line)


def parse_ulp_file(
    path: str,
    possible_delimiters: Iterable[str] = (":", "|"),
    on_success: Callable[[str, str, str], None] = None,
    on_error: Callable[[str, int, str], None] = None,
    fs: fsspec.AbstractFileSystem = fsspec.filesystem("file")
) -> None:
    """
    Parse a ulp file line by line.

    Parameters:
    - on_success(host, login, password): called for successfully parsed lines
    - on_error(error_type, lineno, line): called for errors ('bad_format' or 'ambiguous')
    """
    with fs.open(path, "r", encoding="utf-8", errors="ignore") as f:
        for lineno, raw in enumerate(f, start=1):
            parse_ulp_line(raw.rstrip("\n\r"), lineno, possible_delimiters, on_success, on_error)


#1

reserved_windows_filenames = {
    "CON", "PRN", "AUX", "NUL",
    *{f"COM{i}" for i in range(1, 10)},
    *{f"LPT{i}" for i in range(1, 10)}
}
def escape_filename(name: str) -> str:
    if name.upper() in reserved_windows_filenames: return f"[[{name}]]"
    return name
def unescape_filename(name: str) -> str:
    if name[:2] == "[[" and name[-2:] == "]]": return name[2:-2]
    return name

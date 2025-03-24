#1

def shorter(text: str, max: int = 500) -> str:
    if len(text) > max: return f"{text[:max-3]}..."
    return text

def tab(text: str, level: int = 1) -> str:
    return "\n".join(["\t"*level + it for it in text.splitlines()])

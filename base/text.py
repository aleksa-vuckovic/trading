#1

def shorter(text: str, max: int = 500):
    if len(text) > max: return f"{text[:max-3]}..."
    return text

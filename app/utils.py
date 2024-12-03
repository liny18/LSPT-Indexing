# app/utils.py
import re


def extract_terms(text: str) -> list:
    """Tokenizes and normalizes the input text."""
    tokens = re.findall(r"\b\w+\b", text.lower())
    return tokens

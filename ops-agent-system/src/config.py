"""Simplified config - env vars or defaults."""
import os

AI_PROVIDER = os.getenv("AI_PROVIDER", "mock")
AI_MODEL = os.getenv("AI_MODEL", "mock")
AI_API_KEY = os.getenv("AI_API_KEY", "")
AI_BASE_URL = os.getenv("AI_BASE_URL", "")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

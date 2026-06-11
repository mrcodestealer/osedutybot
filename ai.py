"""Backward compatibility — ``ai.py`` was renamed to ``commandagent.py``."""

from commandagent import *  # noqa: F403
from commandagent import main

if __name__ == "__main__":
    main()

# evidence_capture.py
import re, sys
import time
import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException

from mss import mss
from PIL import Image
import os
# ===================== CONFIG =====================
EXCEL_FILE = r"E:\NCS\fb-selenium\util\output_links.xlsx"
SHEET_NAME = "comment_content"

COL_LINK = "facebook_link"
COL_AUTHOR = "author_comment"
COL_COMMENT = "comment_content"
COL_UID = "author_id"  

OUT_DIR = Path("evidence_screenshots")
OUT_DIR.mkdir(exist_ok=True)

NDJSON_FILE = OUT_DIR / "results.ndjson"

HEADLESS = False
MAX_ROUNDS = 1000
SLEEP_BETWEEN_ROUNDS = 1.0

SCROLL_STEP = 360

SCROLL_PAUSE = 0.25
WAIT_AFTER_OPEN_COMMENTS = 1.0
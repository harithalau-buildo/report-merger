"""
Daily Report Merger - Phase 2 Version 3
CORRECTED LOGIC: Filter each file individually BEFORE combining

Human Workflow Simulation:
1. For each SET:
   - Filter OnDuty file by set's accounts -> get subset
   - Filter Productivity file by set's accounts -> get subset
   - Filter UR file by set's accounts -> get subset
   - Filter Accuracy file by set's accounts -> get subset
   - Combine these 4 SUBSETS horizontally -> This becomes the set's sheet
2. Main sheet = combine all sets together

This ensures each set sheet contains ONLY that set's working accounts.

Author: Built for 4lau
Date: April 2026
"""

import pandas as pd
import os
import re
import csv
import unicodedata
from datetime import datetime

# ============================================================================
# CONFIGURATION
# ============================================================================

def _parse_env_value(raw_value):
    """Parse .env value, including optional Python-like raw string notation."""
    value = raw_value.strip()
    if not value:
        return ""

    # Support values like r"C:\path\to\file"
    if (value.startswith('r"') and value.endswith('"')) or (
        value.startswith("r'") and value.endswith("'")
    ):
        value = value[2:-1]
    elif (value.startswith('"') and value.endswith('"')) or (
        value.startswith("'") and value.endswith("'")
    ):
        value = value[1:-1]

    return os.path.expandvars(value)


def load_env_config(env_path):
    """Load key/value pairs from a simple .env file."""
    config = {}
    if not os.path.exists(env_path):
        return config

    with open(env_path, "r", encoding="utf-8-sig") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip()
            config[key] = _parse_env_value(value)
    return config


def _to_bool(value, default=False):
    """Convert string/bool env values to bool."""
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_FILE = os.path.join(SCRIPT_DIR, ".env")
ENV_CONFIG = load_env_config(ENV_FILE)

REQUIRED_ENV_KEYS = [
    "SRC_FOLDER",
    "CLEANED_SRC_FOLDER",
    "OUTPUT_FOLDER",
    "OUTPUT_FILENAME",
    "PROJECT_DETAILS_FILE",
]

SRC_FOLDER = ENV_CONFIG.get("SRC_FOLDER")
CLEANED_SRC_FOLDER = ENV_CONFIG.get("CLEANED_SRC_FOLDER")
OUTPUT_FOLDER = ENV_CONFIG.get("OUTPUT_FOLDER")
OUTPUT_FILENAME = ENV_CONFIG.get("OUTPUT_FILENAME")
PROJECT_DETAILS_FILE = ENV_CONFIG.get("PROJECT_DETAILS_FILE")

LARK_SAFE_EXPORT = _to_bool(ENV_CONFIG.get("LARK_SAFE_EXPORT"), default=True)
CHAR_AUDIT_REPORT = _to_bool(ENV_CONFIG.get("CHAR_AUDIT_REPORT"), default=True)
STRICT_LEADING_SPACE_TRIM = _to_bool(ENV_CONFIG.get("STRICT_LEADING_SPACE_TRIM"), default=True)
PRE_CLEAN_SOURCE_CSV = _to_bool(ENV_CONFIG.get("PRE_CLEAN_SOURCE_CSV"), default=True)
PRE_CLEAN_MODE = str(ENV_CONFIG.get("PRE_CLEAN_MODE", "leading_ascii_only")).strip().lower()
KEEP_PRE_CLEANED_FILES = _to_bool(ENV_CONFIG.get("KEEP_PRE_CLEANED_FILES"), default=True)

WORKBOOK_README = (
    "Read me:\n"
    "1. Avg Working Hours=Label Tasks Hours+Non-label Task Hours+Offline Working "
    "Hours+Meeting Hours+Training Hours+Idle Hours\n"
    "2. Effective Pro Hour: the time that all operation(clicking or sliding or "
    "pressing) auto captured by the system on labeling page\n"
    "3.Utilization Rate(>=75%): Avg Effective Pro Hour/Avg Working Hours"
)


def validate_required_env_config():
    """Return list of required .env keys that are missing or empty."""
    missing = []
    for key in REQUIRED_ENV_KEYS:
        value = ENV_CONFIG.get(key)
        if value is None or str(value).strip() == "":
            missing.append(key)
    return missing

def normalize_account(value):
    """Normalize account/email values for strict set filtering."""
    if pd.isna(value):
        return ""
    return str(value).strip().lower()


def clean_text_value(value):
    """Remove hidden leading characters and trim text for safer cross-app copy/paste."""
    if not isinstance(value, str):
        return value
    # Normalize visually similar Unicode and then remove hidden/problematic chars.
    cleaned = unicodedata.normalize("NFKC", value)
    cleaned = (
        cleaned.replace("\ufeff", "")  # BOM
        .replace("\u00a0", " ")        # NBSP
        .replace("\u200b", "")         # zero-width space
        .replace("\u200c", "")         # ZWNJ
        .replace("\u200d", "")         # ZWJ
        .replace("\u2060", "")         # word joiner
        .replace("\u200e", "")         # LRM
        .replace("\u200f", "")         # RLM
        .replace("\u202a", "")         # LRE
        .replace("\u202b", "")         # RLE
        .replace("\u202c", "")         # PDF
        .replace("\u202d", "")         # LRO
        .replace("\u202e", "")         # RLO
    )
    # Drop C0 control chars but keep line breaks/tabs if they are intentional.
    cleaned = re.sub(r"[\x00-\x08\x0B-\x1F\x7F]", "", cleaned)
    cleaned = cleaned.strip()
    if STRICT_LEADING_SPACE_TRIM:
        cleaned = re.sub(r"^\s+", "", cleaned)
    cleaned = re.sub(r"[ \t]{2,}", " ", cleaned)
    return cleaned


def sanitize_dataframe_text(df):
    """Sanitize headers and string cells to avoid hidden-space issues in downstream tools."""
    if df is None or df.empty:
        return df

    clean_df = df.copy()
    clean_df.columns = [clean_text_value(str(col)) for col in clean_df.columns]

    object_columns = clean_df.select_dtypes(include=["object", "string"]).columns
    for col in object_columns:
        clean_df[col] = clean_df[col].apply(clean_text_value)

    return clean_df


def enforce_final_ascii_lstrip(df):
    """Final guard: remove leading whitespace gaps from all text cells."""
    if df is None or df.empty:
        return df, 0

    clean_df = df.copy()
    changes = 0
    object_columns = clean_df.select_dtypes(include=["object", "string"]).columns
    for col_name in object_columns:
        original = clean_df[col_name]
        if original.empty:
            continue

        mask = original.apply(lambda v: isinstance(v, str) and bool(re.match(r"^\s+", v)))
        if not mask.any():
            continue

        clean_df.loc[mask, col_name] = original.loc[mask].apply(clean_text_value)
        changes += int(mask.sum())
    return clean_df, changes


def _leading_whitespace_count(text):
    """Count leading whitespace characters."""
    count = 0
    for ch in text:
        if ch.isspace():
            count += 1
        else:
            break
    return count


def collect_suspicious_text_cells(df, sheet_name, max_rows=200):
    """Collect rows with potentially problematic leading characters."""
    findings = []
    if df is None or df.empty:
        return findings

    sample = df.head(max_rows)
    obj_cols = sample.select_dtypes(include=["object", "string"]).columns
    for row_idx, row in sample[obj_cols].iterrows():
        for col in obj_cols:
            value = row[col]
            if not isinstance(value, str) or value == "":
                continue
            first = value[0]
            lead_ws = _leading_whitespace_count(value)
            cat = unicodedata.category(first)
            code = ord(first)

            suspicious = (
                lead_ws > 0
                or code in {9, 32, 160, 65279, 8203, 8204, 8205, 8288}
                or cat.startswith("C")
                or cat == "Zs"
            )
            if suspicious:
                findings.append(
                    {
                        "sheet": sheet_name,
                        "row_index": int(row_idx),
                        "column": str(col),
                        "first_char_code": code,
                        "first_char_unicode": unicodedata.name(first, "UNKNOWN"),
                        "leading_whitespace_count": lead_ws,
                        "preview": value[:80],
                    }
                )
    return findings


def load_project_details(filepath):
    """
    Read project details from text file.

    Expected format:
    - First non-empty line: report date (MM-DD-YYYY)
    - Then repeated blocks separated by blank lines:
      line 1: set name
      next line(s): comma-separated working accounts
    """
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Project details file not found: {filepath}")

    with open(filepath, "r", encoding="utf-8-sig") as f:
        raw_lines = [line.rstrip() for line in f]

    # Keep original blank lines for block separation, but trim surrounding spaces
    lines = [line.strip() for line in raw_lines]

    # First non-empty line is date
    non_empty = [line for line in lines if line]
    if not non_empty:
        raise ValueError("Project details file is empty.")

    report_date = non_empty[0]
    if not (len(report_date) == 10 and report_date[2] == "-" and report_date[5] == "-"):
        raise ValueError(
            "Invalid date in projectdetails.txt. Expected MM-DD-YYYY on first line."
        )

    # Remaining content after the first date line
    start_idx = lines.index(report_date) + 1
    remaining = lines[start_idx:]

    # Split into blocks by blank lines
    blocks = []
    current = []
    for line in remaining:
        if line:
            current.append(line)
        elif current:
            blocks.append(current)
            current = []
    if current:
        blocks.append(current)

    sets = []
    for block in blocks:
        if len(block) < 2:
            continue
        set_name = clean_text_value(block[0])
        accounts_text = " ".join(block[1:])
        accounts = [
            clean_text_value(email)
            for email in accounts_text.split(",")
            if clean_text_value(email)
        ]
        if accounts:
            sets.append({"name": set_name, "accounts": accounts})

    if not sets:
        raise ValueError("No valid set/account blocks found in projectdetails.txt.")

    return report_date, sets


def trim_cell_value(value, mode="leading_ascii_only"):
    """Trim source cell value according to mode."""
    if not isinstance(value, str):
        return value
    if mode == "leading_ascii_only":
        return value.lstrip()
    return value.lstrip()


def write_dataframe_to_xlsx(df, output_path, sheet_name="Sheet1", header=True):
    """Write dataframe to an xlsx file with plain text-friendly formatting."""
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name=sheet_name, index=False, header=header)
        worksheet = writer.sheets[sheet_name]
        sanitize_worksheet_text_cells(worksheet, start_row=1)


def write_audit_xlsx(rows, output_path, sheet_name="Audit"):
    """Write audit rows to xlsx."""
    pd.DataFrame(rows).to_excel(output_path, index=False, sheet_name=sheet_name)


def preclean_source_file_to_xlsx(input_path, output_path, mode="leading_ascii_only", sample_limit=20):
    """Create pre-cleaned xlsx copy from csv/xlsx source and return cleaning stats."""
    cells_total = 0
    cells_changed = 0
    samples = []
    cleaned_rows = []

    if input_path.lower().endswith(".csv"):
        with open(input_path, "r", encoding="utf-8-sig", newline="") as src:
            reader = csv.reader(src)
            for row_idx, row in enumerate(reader, start=1):
                cleaned_row = []
                for col_idx, cell in enumerate(row, start=1):
                    cells_total += 1
                    cleaned = trim_cell_value(cell, mode=mode)
                    cleaned = clean_text_value(cleaned)
                    if cleaned != cell:
                        cells_changed += 1
                        if len(samples) < sample_limit:
                            samples.append(
                                {
                                    "row": row_idx,
                                    "column": col_idx,
                                    "original_preview": str(cell)[:80],
                                    "cleaned_preview": str(cleaned)[:80],
                                }
                            )
                    cleaned_row.append(cleaned)
                cleaned_rows.append(cleaned_row)
    else:
        raw_df = pd.read_excel(
            input_path,
            sheet_name=0,
            header=None,
            dtype=str,
            keep_default_na=False,
        )
        for row_idx in range(len(raw_df)):
            out_row = []
            for col_idx, value in enumerate(raw_df.iloc[row_idx].tolist(), start=1):
                text_value = "" if value is None else str(value)
                cells_total += 1
                cleaned = trim_cell_value(text_value, mode=mode)
                cleaned = clean_text_value(cleaned)
                if cleaned != text_value:
                    cells_changed += 1
                    if len(samples) < sample_limit:
                        samples.append(
                            {
                                "row": row_idx + 1,
                                "column": col_idx,
                                "original_preview": text_value[:80],
                                "cleaned_preview": cleaned[:80],
                            }
                        )
                out_row.append(cleaned)
            cleaned_rows.append(out_row)

    write_dataframe_to_xlsx(pd.DataFrame(cleaned_rows), output_path, header=False)

    return {
        "cells_total": cells_total,
        "cells_changed": cells_changed,
        "samples": samples,
    }


def preclean_selected_source_files(files, output_folder, mode="leading_ascii_only", keep_files=True):
    """Pre-clean selected source CSV files and return updated file mapping."""
    run_stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    preclean_dir = os.path.join(output_folder, f"_precleaned_sources_{run_stamp}")
    os.makedirs(preclean_dir, exist_ok=True)

    cleaned_files = {}
    audit_rows = []
    summaries = []

    for file_type, src_path in files.items():
        if not src_path:
            continue

        out_name = os.path.basename(src_path)
        out_path = os.path.join(preclean_dir, out_name)
        stats = preclean_source_file_to_xlsx(src_path, out_path, mode=mode)

        cleaned_files[file_type] = out_path
        summaries.append(
            {
                "file_type": file_type,
                "source_file": src_path,
                "cleaned_file": out_path,
                "cells_total": stats["cells_total"],
                "cells_changed": stats["cells_changed"],
            }
        )
        for sample in stats["samples"]:
            audit_rows.append(
                {
                    "file_type": file_type,
                    "source_file": os.path.basename(src_path),
                    "row": sample["row"],
                    "column": sample["column"],
                    "original_preview": sample["original_preview"],
                    "cleaned_preview": sample["cleaned_preview"],
                }
            )

    audit_path = os.path.join(
        output_folder,
        "dailycleaned_source_trim_audit.xlsx",
    )
    write_audit_xlsx(audit_rows, audit_path)

    if not keep_files:
        # Keep folder for current run regardless; caller can disable retention in future iteration.
        pass

    return cleaned_files, summaries, audit_path, preclean_dir


def _clear_folder_files(folder_path):
    """Delete files in a folder (non-recursive)."""
    for name in os.listdir(folder_path):
        path = os.path.join(folder_path, name)
        if os.path.isfile(path):
            os.remove(path)


def clean_excel_sources_to_cleaned_src(src_folder, cleaned_src_folder, mode="leading_ascii_only"):
    """
    Read source files (Excel or CSV) from src_folder, clean cell text,
    and export cleaned xlsx files to cleaned_src_folder.
    Returns list of created xlsx paths and summary stats.
    """
    os.makedirs(cleaned_src_folder, exist_ok=True)
    _clear_folder_files(cleaned_src_folder)

    excel_files = [
        f
        for f in os.listdir(src_folder)
        if f.lower().endswith((".xlsx", ".xlsm", ".xls"))
    ]
    csv_files = [f for f in os.listdir(src_folder) if f.lower().endswith(".csv")]
    source_files = sorted(excel_files + csv_files)

    if len(source_files) < 4:
        raise ValueError(
            f"Found only {len(source_files)} source files in src. Need at least 4."
        )

    created_xlsx_paths = []
    summary = []
    audit_rows = []

    for filename in source_files:
        src_path = os.path.join(src_folder, filename)
        base_name = os.path.splitext(filename)[0]
        out_path = os.path.join(cleaned_src_folder, f"{base_name}.xlsx")
        stats = preclean_source_file_to_xlsx(src_path, out_path, mode=mode, sample_limit=200)
        cells_total = stats["cells_total"]
        cells_changed = stats["cells_changed"]
        for sample in stats["samples"]:
            if len(audit_rows) < 5000:
                audit_rows.append(
                    {
                        "source_file": filename,
                        "row": sample["row"],
                        "column": sample["column"],
                        "original_preview": sample["original_preview"],
                        "cleaned_preview": sample["cleaned_preview"],
                    }
                )

        created_xlsx_paths.append(out_path)
        summary.append(
            {
                "source_file": filename,
                "cleaned_file": os.path.basename(out_path),
                "cells_total": cells_total,
                "cells_changed": cells_changed,
            }
        )

    return created_xlsx_paths, summary, audit_rows

# ============================================================================
# USER INPUT FUNCTIONS
# ============================================================================

def get_date_input():
    """Ask user for report date"""
    print("\n" + "=" * 70)
    print("STEP 1: Enter Report Date")
    print("=" * 70)
    
    while True:
        date_input = input("Enter date (format: MM-DD-YYYY, e.g., 04-01-2026): ").strip()
        if len(date_input) == 10 and date_input[2] == '-' and date_input[5] == '-':
            print(f"[OK] Date set to: {date_input}")
            return date_input
        else:
            print("[ERROR] Invalid format. Please use MM-DD-YYYY")

def get_sets_input():
    """
    Ask user for sets configuration
    Returns list of dicts: [{'name': 'Set A: ...', 'accounts': ['email1', 'email2']}, ...]
    """
    print("\n" + "=" * 70)
    print("STEP 2: Configure Sets")
    print("=" * 70)
    
    while True:
        try:
            num_sets = int(input("How many sets do you have? (e.g., 5): ").strip())
            if num_sets > 0:
                print(f"[OK] You will configure {num_sets} sets")
                break
            else:
                print("[ERROR] Please enter a number greater than 0")
        except ValueError:
            print("[ERROR] Please enter a valid number")
    
    sets = []
    for i in range(num_sets):
        print(f"\n--- Set {i+1} of {num_sets} ---")
        set_name = input(f"Enter name for Set {i+1} (e.g., 'Labelling_Guide To_The Dawn_en-US'): ").strip()
        
        while True:
            accounts_input = input(f"Enter working accounts for '{set_name}' (comma-separated):\n").strip()
            accounts = [email.strip() for email in accounts_input.split(',') if email.strip()]
            if len(accounts) > 0:
                print(f"[OK] Added {len(accounts)} account(s): {', '.join(accounts)}")
                break
            else:
                print("[ERROR] Please enter at least one email")
        
        sets.append({'name': set_name, 'accounts': accounts})
    
    print("\n" + "=" * 70)
    print("Sets Configuration Summary:")
    print("=" * 70)
    for i, s in enumerate(sets, 1):
        print(f"{i}. {s['name']}")
        print(f"   Accounts: {', '.join(s['accounts'])}")
    
    return sets

# ============================================================================
# FILE IDENTIFICATION
# ============================================================================

def identify_file_type(filepath):
    """Identify file type by content"""
    try:
        filename = os.path.basename(filepath).lower()
        if filepath.lower().endswith(".csv"):
            with open(filepath, "r", encoding="utf-8-sig") as f:
                first_lines = "".join([f.readline() for _ in range(20)])
        else:
            preview_df = pd.read_excel(
                filepath,
                sheet_name=0,
                header=None,
                dtype=str,
                keep_default_na=False,
                nrows=20,
            )
            first_lines = "\n".join(
                " ".join(str(value) for value in row if str(value).strip())
                for row in preview_df.fillna("").values.tolist()
            )

        text = first_lines.lower()

        # 1) Productivity
        if 'prod(case/h)' in text or 'total productivity' in filename:
            return 'productivity'

        # 2) Accuracy
        if 'sampled' in text or 'accr' in filename:
            return 'accuracy'

        # 3) UR (check before onduty because UR also contains "rest hrs")
        if (
            'on duty ur' in filename
            or ('on duty' in text and 'ur' in text)
            or ('occupancy' in text and 'eph hrs' in text and 'working hrs' in text)
        ):
            return 'ur'

        # 4) OnDuty data export (your rule: Moderation Task, plus Rest as supporting keyword)
        if (
            'moderation task' in text
            or (
                'rest' in text
                and ('status detail' in text or 'shift type' in text or 'no. of task' in text)
            )
            or (filename.startswith('data-') and ('status detail' in text or 'shift type' in text))
        ):
            return 'onduty'

        return None
    except Exception as e:
        print(f"Error checking {filepath}: {e}")
        return None


def read_table_file(filepath, **kwargs):
    """Read csv/xlsx file with pandas using the appropriate backend."""
    if filepath.lower().endswith(".csv"):
        return pd.read_csv(filepath, encoding="utf-8-sig", **kwargs)
    return pd.read_excel(filepath, sheet_name=0, **kwargs)

# ============================================================================
# FILE LOADING
# ============================================================================

def load_onduty_file(filepath):
    """Load OnDuty file (skip first 2 header rows)"""
    print(f"  Loading: {os.path.basename(filepath)}")
    df = read_table_file(filepath, skiprows=2)
    return sanitize_dataframe_text(df)

def load_productivity_file(filepath):
    """Load Productivity file"""
    print(f"  Loading: {os.path.basename(filepath)}")
    df = read_table_file(filepath)
    return sanitize_dataframe_text(df)

def load_ur_file(filepath):
    """Load UR file (remove Sum row)"""
    print(f"  Loading: {os.path.basename(filepath)}")
    df = read_table_file(filepath)
    df = sanitize_dataframe_text(df)
    if 'Date' in df.columns:
        df = df[df['Date'] != 'Sum']
    else:
        print("    Warning: 'Date' column not found in UR file; skipping Sum-row filter")
    return df

def load_accuracy_file(filepath):
    """Load Accuracy file"""
    print(f"  Loading: {os.path.basename(filepath)}")
    df = read_table_file(filepath)
    return sanitize_dataframe_text(df)

# ============================================================================
# FILTERING FUNCTION - THE KEY FIX
# This filters a SINGLE file by accounts before combining
# ============================================================================

def filter_single_file_by_accounts(df, account_list, file_type):
    """
    Filter a SINGLE file (not combined) by account list

    This is the CORRECT approach:
    - Filter each file individually BEFORE combining
    - Not after combining (which picks up other accounts)

    Args:
        df: Single file dataframe (OnDuty OR Productivity OR UR OR Accuracy)
        account_list: List of emails to filter for
        file_type: 'onduty', 'productivity', 'ur', or 'accuracy'

    Returns:
        Filtered dataframe containing only rows for specified accounts
    """

    normalized_accounts = {normalize_account(acc) for acc in account_list if normalize_account(acc)}

    # Identify which column contains the email/account identifier
    # Different files use different column names
    email_column = None

    if file_type == 'onduty':
        # OnDuty file uses bilingual column "邮箱/Email", but encoding may vary in source files
        possible_cols = ['邮箱/Email', 'é‚®ç®±/Email', 'Email', '邮箱', 'é‚®ç®±']
        for col in possible_cols:
            if col in df.columns:
                email_column = col
                break

    elif file_type == 'productivity':
        # Productivity file uses "Moderator"
        if 'Moderator' in df.columns:
            email_column = 'Moderator'

    elif file_type == 'ur':
        # UR file uses "Name"
        if 'Name' in df.columns:
            email_column = 'Name'

    elif file_type == 'accuracy':
        # Accuracy file uses "Moderator"
        if 'Moderator' in df.columns:
            email_column = 'Moderator'

    # If we found the account column, filter by strict normalized exact match
    if email_column and email_column in df.columns:
        normalized_values = df[email_column].apply(normalize_account)
        filtered_df = df[normalized_values.isin(normalized_accounts)].copy()
        return filtered_df.reset_index(drop=True)

    # If we can't find account column, return empty dataframe with same structure
    print(f"    Warning: Could not find account column in {file_type} file")
    return df.iloc[0:0].copy()
# ============================================================================
# COMBINING FUNCTION - Updated to work with pre-filtered data
# ============================================================================

def concatenate_filtered_files(onduty_df, productivity_df, ur_df, accuracy_df):
    """
    Combine 4 already-filtered dataframes horizontally
    
    These dataframes have already been filtered for a specific set's accounts,
    so we just place them side-by-side.
    """
    combined = pd.concat(
        [
            onduty_df.reset_index(drop=True),
            productivity_df.reset_index(drop=True),
            ur_df.reset_index(drop=True),
            accuracy_df.reset_index(drop=True),
        ],
        axis=1  # Horizontal
    )
    return combined


def sanitize_worksheet_text_cells(worksheet, start_row=1):
    """Sanitize string cells directly in an openpyxl worksheet."""
    changed = 0
    leading_space_fixed = 0
    for row in worksheet.iter_rows(min_row=start_row):
        for cell in row:
            if isinstance(cell.value, str):
                before = cell.value
                cleaned = clean_text_value(cell.value)
                if STRICT_LEADING_SPACE_TRIM:
                    stripped = re.sub(r"^\s+", "", cleaned)
                    if stripped != cleaned:
                        leading_space_fixed += 1
                    cleaned = stripped
                if cleaned != cell.value:
                    cell.value = cleaned
                    changed += 1
                # Keep plain formatting for easier downstream clipboard behavior
                cell.number_format = "General"
    return changed, leading_space_fixed


def make_unique_excel_sheet_name(raw_name, used_names):
    """Create an Excel-safe unique worksheet name within the 31-char limit."""
    clean_name = clean_text_value(str(raw_name or "Set"))
    for old, new in ((":", "-"), ("/", "-"), ("\\", "-"), ("?", ""), ("*", ""), ("[", ""), ("]", "")):
        clean_name = clean_name.replace(old, new)

    clean_name = clean_name.strip().strip("'") or "Set"
    base_name = clean_name[:31] or "Set"
    candidate = base_name
    suffix = 2

    while candidate in used_names:
        suffix_text = f"_{suffix}"
        candidate = f"{base_name[:31 - len(suffix_text)]}{suffix_text}".rstrip() or f"Set{suffix_text}"
        suffix += 1

    used_names.add(candidate)
    return candidate

# ============================================================================
# EXCEL SAVING WITH MULTIPLE SHEETS
# ============================================================================

def save_to_excel_with_sheets(all_sets_data, output_path):
    """
    Save to Excel with multiple sheets
    
    Args:
        all_sets_data: List of dicts, each containing:
            - 'name': Set name
            - 'dataframe': The combined data for that set
        output_path: Where to save
    """
    print(f"\nCreating Excel file: {output_path}")

    # Build sanitized copies for output and optional audits
    sanitized_sets_data = []
    excel_sheet_names = {"Main"}
    export_names = set()
    for item in all_sets_data:
        clean_name = clean_text_value(item["name"])
        sanitized_sets_data.append(
            {
                "name": clean_name,
                "sheet_name": make_unique_excel_sheet_name(clean_name, excel_sheet_names),
                "export_name": make_unique_excel_sheet_name(clean_name, export_names),
                "dataframe": sanitize_dataframe_text(item["dataframe"]),
            }
        )

    # Create main dataframe from already-sanitized set data
    all_sets_dfs = [set_data['dataframe'] for set_data in sanitized_sets_data]
    main_df = pd.concat(all_sets_dfs, axis=0, ignore_index=True)
    main_df = sanitize_dataframe_text(main_df)
    main_df, main_fixed = enforce_final_ascii_lstrip(main_df)

    # Character audit report (step 1)
    if CHAR_AUDIT_REPORT:
        audit_rows = collect_suspicious_text_cells(main_df, "Main")
        for set_data in sanitized_sets_data:
            audit_rows.extend(
                collect_suspicious_text_cells(set_data["dataframe"], set_data["name"])
            )
        audit_path = os.path.join(
            os.path.dirname(output_path),
            f"{os.path.splitext(os.path.basename(output_path))[0]}_char_audit.xlsx",
        )
        write_audit_xlsx(audit_rows, audit_path)
        print(f"  Character audit report: {audit_path} ({len(audit_rows)} findings)")
    
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        # First, create the MAIN sheet by combining all sets vertically
        print("\n  Creating 'Main' sheet (all sets combined)...")
        
        print(f"    Rows: {len(main_df)}, Columns: {len(main_df.columns)}")
        
        # Write Main sheet
        main_df.to_excel(writer, sheet_name='Main', index=False, startrow=3)
        
        # Add headers to Main sheet
        worksheet = writer.sheets['Main']
        worksheet['A1'] = 'Timezone:'
        worksheet['A2'] = WORKBOOK_README
        
        # Then create individual set sheets
        set_level_fixes = 0
        for set_data in sanitized_sets_data:
            set_name = set_data['name']
            set_df, fixed_count = enforce_final_ascii_lstrip(set_data['dataframe'])
            set_level_fixes += fixed_count
            set_data['dataframe'] = set_df
            
            print(f"\n  Creating sheet: '{set_name}'")
            print(f"    Rows: {len(set_df)}, Columns: {len(set_df.columns)}")
            
            clean_name = set_data["sheet_name"]
            
            # Write set sheet
            set_df.to_excel(writer, sheet_name=clean_name, index=False, startrow=3)
            
            # Add headers
            worksheet = writer.sheets[clean_name]
            worksheet['A1'] = 'Timezone:'
            worksheet['A2'] = WORKBOOK_README

        # Post-write workbook cleanup (step 3)
        total_changed = 0
        total_leading_space_fixed = 0
        for ws in writer.book.worksheets:
            changed, fixed = sanitize_worksheet_text_cells(ws, start_row=1)
            total_changed += changed
            total_leading_space_fixed += fixed
        print(f"  Post-write text cleanup: {total_changed} cell(s) adjusted")
        if STRICT_LEADING_SPACE_TRIM:
            print(
                f"  Leading ASCII spaces removed: "
                f"{main_fixed + set_level_fixes + total_leading_space_fixed} fix(es)"
            )
    
    print(f"\n  [OK] Excel file created successfully!")
    print(f"  [OK] Total sheets: {1 + len(all_sets_data)} (1 main + {len(all_sets_data)} sets)")

    # Optional sheet-level xlsx exports
    if LARK_SAFE_EXPORT:
        export_dir = os.path.join(
            os.path.dirname(output_path),
            f"{os.path.splitext(os.path.basename(output_path))[0]}_sheet_exports",
        )
        os.makedirs(export_dir, exist_ok=True)

        write_dataframe_to_xlsx(main_df, os.path.join(export_dir, "Main.xlsx"))
        for set_data in sanitized_sets_data:
            write_dataframe_to_xlsx(
                set_data["dataframe"],
                os.path.join(export_dir, f"{set_data['export_name']}.xlsx"),
            )
        print(f"  Additional xlsx exports: {export_dir}")

# ============================================================================
# MAIN FUNCTION
# ============================================================================

def main():
    """Main execution - implements the correct human workflow"""
    
    print("=" * 70)
    print("Daily Report Merger - Phase 2 Version 3")
    print("CORRECTED: Filter-Then-Combine Approach")
    print("=" * 70)

    missing_keys = validate_required_env_config()
    if missing_keys:
        print("\nERROR: Missing required .env configuration keys:")
        for key in missing_keys:
            print(f"  - {key}")
        print(f"\nPlease update: {ENV_FILE}")
        return
    
    # STEP 1-2: Load date + sets from projectdetails.txt
    print("\n" + "=" * 70)
    print("Loading project details from text file...")
    print("=" * 70)
    try:
        report_date, sets_config = load_project_details(PROJECT_DETAILS_FILE)
    except Exception as e:
        print(f"\nERROR: {e}")
        return

    print(f"\nDate: {report_date}")
    print(f"Detected {len(sets_config)} set(s) from projectdetails.txt")
    print("\n" + "=" * 70)
    print("Sets Configuration Summary:")
    print("=" * 70)
    for i, s in enumerate(sets_config, 1):
        print(f"{i}. {s['name']}")
        print(f"   Accounts: {', '.join(s['accounts'])}")
    
    # STEP 3: Check folders
    if not os.path.exists(SRC_FOLDER):
        print(f"\n[ERROR] Source folder not found: {SRC_FOLDER}")
        return

    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    os.makedirs(CLEANED_SRC_FOLDER, exist_ok=True)

    # STEP 4: Clean source Excel files to cleaned_src
    print("\n" + "=" * 70)
    print("Cleaning source files -> cleaned_src...")
    print("=" * 70)

    try:
        created_xlsx_paths, clean_summary, clean_audit_rows = clean_excel_sources_to_cleaned_src(
            SRC_FOLDER,
            CLEANED_SRC_FOLDER,
            mode=PRE_CLEAN_MODE,
        )
    except Exception as e:
        print(f"\n[ERROR] cleaning source files: {e}")
        import traceback
        traceback.print_exc()
        return

    print(f"\n[OK] Cleaned {len(created_xlsx_paths)} source file(s) into: {CLEANED_SRC_FOLDER}")
    for item in clean_summary:
        print(
            f"  {item['source_file']} -> {item['cleaned_file']} "
            f"(changed {item['cells_changed']} / {item['cells_total']} cells)"
        )

    source_audit_path = os.path.join(OUTPUT_FOLDER, "dailycleaned_source_trim_audit.xlsx")
    write_audit_xlsx(clean_audit_rows, source_audit_path)
    print(f"  Source trim audit: {source_audit_path}")

    # STEP 5: Find and identify XLSX files from cleaned_src (newest first)
    print("\n" + "=" * 70)
    print("Finding XLSX files in cleaned_src...")
    print("=" * 70)
    
    cleaned_files = [f for f in os.listdir(CLEANED_SRC_FOLDER) if f.lower().endswith(".xlsx")]
    
    if len(cleaned_files) < 4:
        print(f"[ERROR] Found only {len(cleaned_files)} XLSX files. Need at least 4.")
        return
    
    # Sort by modification time (newest first)
    file_info = []
    for f in cleaned_files:
        filepath = os.path.join(CLEANED_SRC_FOLDER, f)
        mtime = os.path.getmtime(filepath)
        mtime_str = datetime.fromtimestamp(mtime).strftime('%Y-%m-%d %H:%M:%S')
        file_info.append({
            'name': f,
            'path': filepath,
            'modified': mtime_str,
            'timestamp': mtime
        })
    
    file_info.sort(key=lambda x: x['timestamp'], reverse=True)
    
    print(f"\n[OK] Found {len(cleaned_files)} XLSX files (showing most recent):")
    for i, info in enumerate(file_info[:8], 1):
        print(f"  {i}. {info['name']}")
        print(f"     Modified: {info['modified']}")
    
    # STEP 6: Identify file types from newest files
    print(f"\n" + "=" * 70)
    print("Identifying file types...")
    print("=" * 70)
    
    files = {'onduty': None, 'productivity': None, 'ur': None, 'accuracy': None}
    
    for info in file_info:
        file_type = identify_file_type(info['path'])
        if file_type and files[file_type] is None:
            files[file_type] = info['path']
            print(f"  [OK] {file_type.upper()}: {info['name']}")
        if all(v is not None for v in files.values()):
            break
    
    missing = [k for k, v in files.items() if v is None]
    if missing:
        print(f"\n[ERROR] Could not identify: {', '.join(missing)}")
        return
    
    # STEP 7: Confirmation
    print("\n" + "=" * 70)
    print("CONFIRMATION: These files will be processed:")
    print("=" * 70)
    for file_type, filepath in files.items():
        filename = os.path.basename(filepath)
        mtime = os.path.getmtime(filepath)
        mtime_str = datetime.fromtimestamp(mtime).strftime('%Y-%m-%d %H:%M')
        print(f"  {file_type.upper()}: {filename}")
        print(f"     Date: {mtime_str}")
    
    proceed = input("\nDo these look correct? (yes/no): ").strip().lower()
    if proceed not in ['yes', 'y']:
        print("\n[ERROR] Cancelled. Clean up old files and try again.")
        return

    # STEP 8: Load all 4 files
    print("\n" + "=" * 70)
    print("Loading files...")
    print("=" * 70)
    
    try:
        onduty_full = load_onduty_file(files['onduty'])
        productivity_full = load_productivity_file(files['productivity'])
        ur_full = load_ur_file(files['ur'])
        accuracy_full = load_accuracy_file(files['accuracy'])
        print(f"\n[OK] All files loaded!")
    except Exception as e:
        print(f"\n[ERROR] loading files: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # STEP 9: THE CORRECT APPROACH - For each set, filter EACH file, THEN combine
    print("\n" + "=" * 70)
    print("Processing sets (CORRECT METHOD: Filter -> Then Combine)...")
    print("=" * 70)
    
    all_sets_data = []
    
    for i, set_config in enumerate(sets_config, 1):
        set_name = set_config['name']
        accounts = set_config['accounts']
        
        print(f"\n--- Processing Set {i}/{len(sets_config)}: {set_name} ---")
        print(f"Accounts: {', '.join(accounts)}")
        
        # Filter each file individually by this set's accounts
        print("  1. Filtering OnDuty file...")
        onduty_filtered = filter_single_file_by_accounts(onduty_full, accounts, 'onduty')
        print(f"     -> Found {len(onduty_filtered)} rows")
        
        print("  2. Filtering Productivity file...")
        productivity_filtered = filter_single_file_by_accounts(productivity_full, accounts, 'productivity')
        print(f"     -> Found {len(productivity_filtered)} rows")
        
        print("  3. Filtering UR file...")
        ur_filtered = filter_single_file_by_accounts(ur_full, accounts, 'ur')
        print(f"     -> Found {len(ur_filtered)} rows")
        
        print("  4. Filtering Accuracy file...")
        accuracy_filtered = filter_single_file_by_accounts(accuracy_full, accounts, 'accuracy')
        print(f"     -> Found {len(accuracy_filtered)} rows")
        
        # Now combine these 4 filtered subsets horizontally
        print("  5. Combining filtered data horizontally...")
        set_combined = concatenate_filtered_files(
            onduty_filtered,
            productivity_filtered,
            ur_filtered,
            accuracy_filtered
        )
        print(f"     -> Final: {len(set_combined)} rows x {len(set_combined.columns)} columns")
        
        # Store this set's data
        all_sets_data.append({
            'name': set_name,
            'dataframe': set_combined
        })
        
        print(f"  [OK] Set {i} complete!")
    
    # STEP 10: Save to Excel
    print("\n" + "=" * 70)
    print("Creating Excel file...")
    print("=" * 70)
    
    output_filename = OUTPUT_FILENAME
    if not output_filename.lower().endswith(".xlsx"):
        output_filename = f"{os.path.splitext(output_filename)[0]}.xlsx"
    output_path = os.path.join(OUTPUT_FOLDER, output_filename)
    
    try:
        save_to_excel_with_sheets(all_sets_data, output_path)
    except Exception as e:
        print(f"\n[ERROR] saving Excel: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # SUCCESS
    print("\n" + "=" * 70)
    print("[OK] SUCCESS!")
    print("=" * 70)
    print(f"Report Date: {report_date}")
    print(f"File: {output_path}")
    print(f"Total Sheets: {1 + len(sets_config)}")
    print(f"\nSheets created:")
    print(f"  1. Main (all sets combined)")
    for i, s in enumerate(sets_config, 2):
        print(f"  {i}. {s['name']}")
    print("\nEach set sheet contains ONLY that set's working accounts.")
    print("=" * 70)

if __name__ == "__main__":
    main()
    input("\nPress Enter to close...")


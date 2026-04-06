# Daily Report Merger - Phase 2 User Guide

## What's New in Phase 2

Phase 2 adds powerful new features:
- ✅ Interactive prompts when you run the script
- ✅ Generates Excel file (.xlsx) instead of CSV
- ✅ Creates multiple sheets organized by Sets
- ✅ Filters data automatically by working accounts

---

## How to Use Phase 2

### Step 1: Prepare Your Files

Make sure you have 4 CSV files in:
```
C:\Users\User\Desktop\March26 BACKUP\pytrial
```

Files needed:
- OnDuty data (file with random numbers)
- Productivity file (contains "Prod(Case/h)")
- UR file (contains "On duty")
- Accuracy file (contains "Sampled")

### Step 2: Run the Script

Double-click: `daily_report_merger_phase2.py`

### Step 3: Answer the Prompts

**Prompt 1: Enter Report Date**
```
Enter date (format: MM-DD-YYYY, e.g., 04-01-2026): 04-01-2026
```

**Prompt 2: How Many Sets**
```
How many sets do you have? (e.g., 5): 5
```

**Prompt 3-7: Configure Each Set**

For each set, you'll enter:
1. Set name (e.g., "Set A: Intention XYZ")
2. Working accounts (comma-separated emails)

Example:
```
--- Set 1 of 5 ---
Enter name for Set 1: Set A: Intention XYZ
Enter working accounts for 'Set A: Intention XYZ':
jlgk276@apy.tech, jyke635@apy.tech, bbff464@apy.tech

--- Set 2 of 5 ---
Enter name for Set 2: Set B: Production ABC
Enter working accounts for 'Set B: Production ABC':
jxuu635@apy.tech

(and so on...)
```

### Step 4: Get Your Output

The script creates an Excel file:
```
C:\Users\User\Desktop\March26 BACKUP\dailyreportinguse\dailycleaned.xlsx
```

---

## What's in the Excel File

### Sheet 1: "All Data"
- Contains complete dataset
- All 4 files merged horizontally
- Same as Phase 1 output

### Sheet 2+: Set-Specific Sheets
- One sheet per set
- Contains only data for that set's working accounts
- Sheet name = Set name you entered
- Same format: OnDuty → Productivity → UR → Accuracy

---

## Example Usage Session

```
Daily Report Merger - Phase 2

STEP 1: Enter Report Date
Enter date (format: MM-DD-YYYY, e.g., 04-01-2026): 04-01-2026
✓ Date set to: 04-01-2026

STEP 2: Configure Sets
How many sets do you have? (e.g., 5): 5
✓ You will configure 5 sets

--- Set 1 of 5 ---
Enter name for Set 1: Set A: Intention XYZ
Enter working accounts for 'Set A: Intention XYZ':
jlgk276@apy.tech, jyke635@apy.tech, bbff464@apy.tech
✓ Added 3 account(s) to 'Set A: Intention XYZ'

--- Set 2 of 5 ---
Enter name for Set 2: Set B: Production ABC
Enter working accounts for 'Set B: Production ABC':
jxuu635@apy.tech
✓ Added 1 account(s) to 'Set B: Production ABC'

(continue for all 5 sets...)

Finding CSV files...
✓ Found 5 CSV files
  ✓ ONDUTY: data-1775101680_1775101688942.csv
  ✓ PRODUCTIVITY: Total_Productivity_Table-2026-04-02_11-59-30.csv
  ✓ UR: On_Duty_UR-2026-04-02_11-59-21.csv
  ✓ ACCURACY: Label_ACCR_Total-2026-04-02_13-20-28.csv

Loading files...
✓ All files loaded successfully!

Combining data...
✓ Combined into 75 rows × 65 columns

Generating Excel file...
  Creating sheet: 'All Data'
    Rows: 75, Columns: 65
  
  Creating sheet: 'Set A: Intention XYZ'
    Filtering for 3 account(s): jlgk276@apy.tech, jyke635@apy.tech, bbff464@apy.tech
    Rows: 18, Columns: 65
  
  Creating sheet: 'Set B: Production ABC'
    Filtering for 1 account(s): jxuu635@apy.tech
    Rows: 8, Columns: 65

(continue for all sets...)

✓ Excel file created successfully!
✓ Total sheets: 6 (1 main + 5 sets)

✓✓✓ SUCCESS! ✓✓✓
Report Date: 04-01-2026
File Location: C:\Users\User\Desktop\March26 BACKUP\dailyreportinguse\dailycleaned.xlsx
Total Sheets: 6

Sheets created:
  1. All Data (complete dataset)
  2. Set A: Intention XYZ
  3. Set B: Production ABC
  4. Set C: Labelling POU
  5. Set D: Style UYT
  6. Set E: OK Content (Life)
```

---

## Time Savings Estimate

**Before (Manual):**
- Combine 4 files: ~10 minutes
- Create Set A sheet: ~5 minutes
- Create Set B sheet: ~5 minutes
- Create Set C sheet: ~5 minutes
- Create Set D sheet: ~5 minutes
- Create Set E sheet: ~5 minutes
**Total: ~35 minutes**

**After (Automated):**
- Answer prompts: ~2 minutes
- Script runs: ~10 seconds
**Total: ~2 minutes**

**Daily time saved: 33 minutes**
**Weekly time saved: 2.75 hours**

---

## Tips for Efficient Use

1. **Save a template file** with your standard set names and accounts
   - Keep a text file with your typical configuration
   - Copy-paste when prompted

2. **Use consistent naming**
   - "Set A: Intention XYZ" is easier to find than random names

3. **Check the "All Data" sheet first**
   - Make sure all data loaded correctly
   - Then check individual set sheets

4. **Excel sheet names are limited to 31 characters**
   - Script auto-truncates longer names
   - Keep set names concise

---

## Troubleshooting

**Problem: "Could not identify file types"**
→ Make sure all 4 CSV files are in the input folder

**Problem: "A set sheet is empty"**
→ Check if email addresses match exactly (case-sensitive)
→ Verify accounts are in the source data

**Problem: "Excel file won't open"**
→ Make sure you have Excel or LibreOffice installed
→ File might be open in another program - close it first

**Problem: "Can't find certain data in set sheets"**
→ Email filtering is exact match
→ Check for typos in email addresses

---

## What to Present to Your Manager

When showing this tool to your manager:

**Before:**
"I manually export 4 files, copy-paste data into separate sheets for each team, which takes 35 minutes daily."

**After:**
"I built an automated tool that:
- Combines 4 data sources
- Generates team-specific reports
- Reduces 35 minutes to 2 minutes
- Eliminates copy-paste errors
- Creates consistent formatting

**Impact: Saving 2.75 hours per week, which I can redirect to [strategic APM work].**"

This demonstrates:
✅ Process improvement mindset (Senior APM skill)
✅ Technical capability (automation)
✅ Time management (efficiency focus)
✅ Scalability (tool can be shared with team)

---

## Next Steps

Once Phase 2 is working:

**Potential Phase 3 enhancements:**
- Auto-formatting (colored headers, borders)
- Auto-calculations (totals, averages per set)
- Charts/graphs generation
- Email automation (auto-send reports)
- Integration with Lark API (auto-upload)

For now, test Phase 2 thoroughly with real data!

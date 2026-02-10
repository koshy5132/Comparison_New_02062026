import os
import pandas as pd
from collections import OrderedDict

# --------------------- Setup ---------------------
# Get current script directory
script_directory = os.path.dirname(os.path.abspath(__file__))

# Define folders for raw CSVs, input Excel reports, and processed outputs
raw_data_folder = os.path.join(script_directory, "rawData")
report_folder = os.path.join(script_directory, "Report")
processed_folder = os.path.join(script_directory, "Processed files")
os.makedirs(processed_folder, exist_ok=True)

# Columns to select from processed Excel reports
columns_to_select = ["Transaction Name", "Average", "90 Percent", "Pass", "Fail", "Stop"]

# Columns to keep from raw CSV files (you can modify this)
raw_keep_columns = ['Transaction Name','Response Time','Pass','Fail','Stop']

# --------------------- Utility Functions ---------------------

# Checks that file path is inside the script folder to avoid unsafe access
def is_safe_file_path(file_path):
    return os.path.commonpath([script_directory, file_path]) == script_directory

# Autofit Excel worksheet columns based on content length
def autofit_worksheet_columns(worksheet, dataframe):
    for idx, col in enumerate(dataframe.columns):
        series = dataframe[col].astype(str)
        max_len = max(series.map(len).max(), len(str(col))) + 2
        worksheet.set_column(idx, idx, max_len)

# Safely convert numeric columns, removing commas and % symbols
def safe_float_conversion(series):
    return pd.to_numeric(series.astype(str).str.replace(',','').str.replace('%',''), errors='coerce')

# --------------------- Load Files ---------------------

# Get the 2 oldest Excel reports from Report folder
xls_files = sorted(
    [os.path.join(report_folder, f) for f in os.listdir(report_folder)
     if f.endswith(('.xls', '.xlsx')) and is_safe_file_path(os.path.join(report_folder, f))],
    key=os.path.getmtime
)[:2]

# Load script-to-keyword mapping (for creating sheets based on scripts)
sheet_mapping_file = os.path.join(script_directory, "script_sheet_mapping.txt")
script_sheet_map = {}
with open(sheet_mapping_file, 'r') as f:
    for line in f:
        if ':' in line:
            sheet_name, keyword = line.strip().split(':', 1)
            script_sheet_map[sheet_name.strip()] = keyword.strip()

# Load pattern mapping (for transaction summary)
pattern_mapping_file = os.path.join(script_directory, "pattern_mapping.txt")
pattern_map = {}
with open(pattern_mapping_file, 'r') as f:
    for line in f:
        if ':' in line:
            sheet_name, pattern = line.strip().split(':', 1)
            pattern_map[sheet_name.strip()] = pattern.strip().lower()

# --------------------- Functions ---------------------

# Process each Excel file to extract relevant transaction data
def process_excel_file(xls_file):
    xls = pd.ExcelFile(xls_file)
    df = pd.read_excel(xls, xls.sheet_names[0], header=None)

    # Find start and end rows of transaction table
    start_idx = end_idx = None
    for idx, row in df.iterrows():
        if row.astype(str).str.contains('Transaction Name', na=False).any():
            start_idx = idx
            break
    for idx, row in df.iterrows():
        if row.astype(str).str.contains('Codes', na=False).any():
            end_idx = idx
            break
    end_idx = end_idx if end_idx else len(df)

    if start_idx is None:
        return None, pd.DataFrame(), None

    filtered_data = df.iloc[start_idx:end_idx]
    header_row = filtered_data.iloc[0]
    filtered_data.columns = header_row
    selected_data = filtered_data[columns_to_select].iloc[1:]
    selected_data['Source File'] = os.path.basename(xls_file)
    sorted_data = selected_data.sort_values(by="Transaction Name").reset_index(drop=True)

    # Exclude non-transaction rows for reference
    excluded_data = df.drop(filtered_data.index).reset_index(drop=True)
    for idx, row in excluded_data.iterrows():
        if str(row.iloc[0]).startswith("Period:"):
            excluded_data = excluded_data.iloc[idx:].reset_index(drop=True)
            break

    name, _ = os.path.splitext(os.path.basename(xls_file))
    output_filename = f"Processed_{name}.xlsx"
    output_path = os.path.join(processed_folder, output_filename)
    with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
        sorted_data.to_excel(writer, sheet_name='Processed Data', index=False)
        excluded_data.to_excel(writer, sheet_name='Excluded Data', index=False)

    return sorted_data, excluded_data, output_filename

# Load all CSV files in RawData folder, keep only selected columns
def load_all_csvs_in_folder():
    csv_files = [f for f in os.listdir(raw_data_folder) if f.lower().endswith('.csv')]
    dataframes = {}
    for f in csv_files:
        try:
            df = pd.read_csv(os.path.join(raw_data_folder, f), header=0, dtype=str, low_memory=False)
            df = df[[c for c in raw_keep_columns if c in df.columns]]
            dataframes[f] = df
        except Exception as e:
            print(f"Failed to load {f}: {e}")
    return dataframes

# --------------------- Main Processing ---------------------

output_files = []
excluded_dataframes = []

for f in xls_files:
    processed, excluded, out_file = process_excel_file(f)
    if out_file:
        output_files.append(out_file)
        excluded_dataframes.append(excluded)

if len(output_files) != 2:
    print("Less than 2 Excel files found in 'Report'. Exiting.")
    exit()

# Read processed baseline and newcode data
df1 = pd.read_excel(os.path.join(processed_folder, output_files[0]), sheet_name='Processed Data')
df2 = pd.read_excel(os.path.join(processed_folder, output_files[1]), sheet_name='Processed Data')
df1 = df1[['Transaction Name','Source File'] + columns_to_select[1:]]
df2 = df2[['Transaction Name','Source File'] + columns_to_select[1:]]

# Convert numeric columns safely
for df in [df1, df2]:
    for col in ['Pass','Fail','Stop','Average','90 Percent']:
        df[col] = safe_float_conversion(df[col])

# Remove duplicates and aggregate metrics
df1 = df1.groupby('Transaction Name', as_index=False).agg({
    'Source File': 'first','Average': 'mean','90 Percent': 'mean',
    'Pass':'sum','Fail':'sum','Stop':'sum'
})
df2 = df2.groupby('Transaction Name', as_index=False).agg({
    'Source File': 'first','Average': 'mean','90 Percent': 'mean',
    'Pass':'sum','Fail':'sum','Stop':'sum'
})

raw_csv_dict = load_all_csvs_in_folder()

# --------------------- Write Excel ---------------------
comparison_file = os.path.join(script_directory,'comparison.xlsx')
all_sheet_names = OrderedDict()

with pd.ExcelWriter(comparison_file, engine='xlsxwriter') as writer:
    workbook = writer.book
    link_fmt = workbook.add_format({'font_color': 'blue','underline':1})

    # Index sheet first
    index_df = pd.DataFrame({'Sheet Name': []})
    index_df.to_excel(writer, sheet_name='Index', index=False)
    ws_index = writer.sheets['Index']
    all_sheet_names['Index'] = 'Index'

    green_format = workbook.add_format({'bg_color': '#EBF1DE'})
    orange_format = workbook.add_format({'bg_color': '#FFA500'})
    red_format = workbook.add_format({'bg_color': '#FF5D5D'})

    # --------------------- Script Sheets ---------------------
    for sheet_name, keyword in script_sheet_map.items():
        truncated_name = sheet_name[:31]
        if truncated_name in all_sheet_names:  # skip duplicates
            continue
        all_sheet_names[truncated_name] = sheet_name

        mask_baseline = df1['Transaction Name'].str.lower().str.contains(keyword.replace('%','').lower())
        mask_newcode = df2['Transaction Name'].str.lower().str.contains(keyword.replace('%','').lower())
        df_script = pd.merge(df1[mask_baseline], df2[mask_newcode],
                             on='Transaction Name', how='outer',
                             suffixes=('_Baseline','_NewCode')).drop_duplicates()

        df_script['Avg_Diff'] = df_script['Average_NewCode'] - df_script['Average_Baseline']
        df_script['90 Percentile Diff'] = (df_script['90 Percent_NewCode'] - df_script['90 Percent_Baseline']) / df_script['90 Percent_Baseline'].replace({0: pd.NA})

        df_script.to_excel(writer, sheet_name=truncated_name, index=False, startrow=1)
        ws = writer.sheets[truncated_name]
        ws.write_url(0,0,"internal:'Index'!A1", cell_format=link_fmt,string="Back to Index")
        ws.freeze_panes(1,1)
        autofit_worksheet_columns(ws, df_script)

        # Conditional formatting for Avg_Diff and 90 Percentile Diff
        if 'Avg_Diff' in df_script.columns:
            avg_col = df_script.columns.get_loc('Avg_Diff')
            for row in range(2, 2 + len(df_script)):
                ws.conditional_format(row, avg_col, row, avg_col, {'type':'cell','criteria':'<','value':0,'format':green_format})
                ws.conditional_format(row, avg_col, row, avg_col, {'type':'formula','criteria':f'=ABS(${"{:c}".format(65+avg_col)}${row+1})<0.1','format':green_format})
                ws.conditional_format(row, avg_col, row, avg_col, {'type':'formula','criteria':f'=AND(ABS(${"{:c}".format(65+avg_col)}${row+1})>=0.1,ABS(${"{:c}".format(65+avg_col)}${row+1})<=0.25)','format':orange_format})
                ws.conditional_format(row, avg_col, row, avg_col, {'type':'formula','criteria':f'=ABS(${"{:c}".format(65+avg_col)}${row+1})>0.25','format':red_format})

        if '90 Percentile Diff' in df_script.columns:
            ninty_col = df_script.columns.get_loc('90 Percentile Diff')
            for row in range(2, 2 + len(df_script)):
                ws.conditional_format(row, ninty_col, row, ninty_col, {'type':'cell','criteria':'<','value':0,'format':green_format})
                ws.conditional_format(row, ninty_col, row, ninty_col, {'type':'formula','criteria':f'=ABS(${"{:c}".format(65+ninty_col)}${row+1})<0.1','format':green_format})
                ws.conditional_format(row, ninty_col, row, ninty_col, {'type':'formula','criteria':f'=AND(ABS(${"{:c}".format(65+ninty_col)}${row+1})>=0.1,ABS(${"{:c}".format(65+ninty_col)}${row+1})<=0.25)','format':orange_format})
                ws.conditional_format(row, ninty_col, row, ninty_col, {'type':'formula','criteria':f'=ABS(${"{:c}".format(65+ninty_col)}${row+1})>0.25','format':red_format})

    # --------------------- Baseline & NewCode Info ---------------------
    for df, name in zip(excluded_dataframes, ['Baseline_Info','NewCode_Info']):
        truncated_name = name[:31]
        if truncated_name not in all_sheet_names:
            all_sheet_names[truncated_name] = name
        df.to_excel(writer, sheet_name=truncated_name, index=False)
        ws = writer.sheets[truncated_name]
        ws.write_url(0,0,"internal:'Index'!A1", cell_format=link_fmt,string="Back to Index")
        autofit_worksheet_columns(ws, df)

    # --------------------- Raw CSV Sheets ---------------------
    for idx, (filename, df) in enumerate(raw_csv_dict.items(), start=1):
        truncated_name = f"{filename[:25]}_{idx}".replace(".","_")
        all_sheet_names[truncated_name] = filename
        df.to_excel(writer, sheet_name=truncated_name, index=False)
        ws = writer.sheets[truncated_name]
        ws.write_url(0,0,"internal:'Index'!A1", cell_format=link_fmt,string="Back to Index")
        ws.freeze_panes(1,0)
        autofit_worksheet_columns(ws, df)

    # --------------------- Pattern Summary ---------------------
    pattern_summary = []
    for sheet_name, pattern in pattern_map.items():
        keyword_lower = script_sheet_map.get(sheet_name,'').replace('%','').lower()
        mask_baseline = df1['Transaction Name'].str.lower().str.contains(keyword_lower)
        mask_newcode = df2['Transaction Name'].str.lower().str.contains(keyword_lower)
        df_script = pd.merge(df1[mask_baseline], df2[mask_newcode],
                             on='Transaction Name', how='outer', suffixes=('_Baseline','_NewCode'))
        df_pattern = df_script[df_script['Transaction Name'].str.lower().str.contains(pattern)]
        for _, row in df_pattern.iterrows():
            pattern_summary.append({
                'Script Name': sheet_name,
                'Transaction Name': row['Transaction Name'],
                'Sum Baseline': row.get('Pass_Baseline',0)+row.get('Fail_Baseline',0)+row.get('Stop_Baseline',0),
                'Sum NewCode': row.get('Pass_NewCode',0)+row.get('Fail_NewCode',0)+row.get('Stop_NewCode',0),
                'Difference': (row.get('Pass_NewCode',0)+row.get('Fail_NewCode',0)+row.get('Stop_NewCode',0)) -
                              (row.get('Pass_Baseline',0)+row.get('Fail_Baseline',0)+row.get('Stop_Baseline',0))
            })
    if pattern_summary:
        df_pattern_summary = pd.DataFrame(pattern_summary)
        df_pattern_summary.to_excel(writer, sheet_name='Pattern_Summary', index=False)
        ws = writer.sheets['Pattern_Summary']
        ws.write_url(0,0,"internal:'Index'!A1", cell_format=link_fmt,string="Back to Index")
        autofit_worksheet_columns(ws, df_pattern_summary)
        all_sheet_names['Pattern_Summary']='Pattern_Summary'

    # --------------------- Instruction Sheet ---------------------
    instruction_text = [
        "Performance Analysis Report - Instructions",
        "",
        "1. Index sheet contains hyperlinks to all other sheets.",
        "2. Script sheets show baseline vs new code metrics for each transaction.",
        "   - Columns Avg_Diff and 90 Percentile Diff are color-coded:",
        "       Green = improvement, Orange = slight change, Red = high difference",
        "3. Baseline_Info and NewCode_Info sheets provide excluded data and file reference info.",
        "4. Raw CSV sheets include transaction raw data from the RawData folder (only selected columns).",
        "5. Pattern_Summary shows user-defined transaction patterns, total counts, and difference baseline vs new code.",
        "6. Use the Back to Index links to navigate easily.",
        "7. This report helps quickly identify performance regressions or improvements across scripts."
    ]
    df_instr = pd.DataFrame({'Instructions': instruction_text})
    df_instr.to_excel(writer, sheet_name='Instructions', index=False)
    ws_instr = writer.sheets['Instructions']
    autofit_worksheet_columns(ws_instr, df_instr)
    all_sheet_names['Instructions']='Instructions'

    # --------------------- Update Index ---------------------
    index_df = pd.DataFrame({'Sheet Name': list(all_sheet_names.keys())})
    index_df.to_excel(writer, sheet_name='Index', index=False)
    ws_index = writer.sheets['Index']
    for row_idx, sheet_name in enumerate(all_sheet_names.keys()):
        ws_index.write_url(row_idx, 0, f"internal:'{sheet_name}'!A1", cell_format=link_fmt,string=sheet_name)
    autofit_worksheet_columns(ws_index, index_df)

print("comparison.xlsx generated successfully with Instructions sheet.")

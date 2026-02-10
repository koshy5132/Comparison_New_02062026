import os
import pandas as pd

# --------------------- Setup ---------------------
script_directory = os.path.dirname(os.path.abspath(__file__))

raw_data_folder = os.path.join(script_directory, "rawData")
report_folder = os.path.join(script_directory, "Report")
processed_folder = os.path.join(script_directory, "Processed files")
os.makedirs(processed_folder, exist_ok=True)

columns_to_select = ["Transaction Name", "Average", "90 Percent", "Pass", "Fail", "Stop"]

def is_safe_file_path(file_path):
    return os.path.commonpath([script_directory, file_path]) == script_directory

def autofit_worksheet_columns(worksheet, dataframe):
    for idx, col in enumerate(dataframe.columns):
        series = dataframe[col].astype(str)
        max_len = max(series.map(len).max(), len(str(col))) + 2
        worksheet.set_column(idx, idx, max_len)

def safe_float_conversion(series):
    return pd.to_numeric(series.astype(str).str.replace(',','').str.replace('%',''), errors='coerce')

# --------------------- Find Oldest Excel Files ---------------------
xls_files = sorted(
    [os.path.join(report_folder, f) for f in os.listdir(report_folder)
     if f.endswith(('.xls', '.xlsx')) and is_safe_file_path(os.path.join(report_folder, f))],
    key=os.path.getmtime
)[:2]

# --------------------- Load Script Sheet Mapping ---------------------
sheet_mapping_file = os.path.join(script_directory, "script_sheet_mapping.txt")
script_sheet_map = {}
with open(sheet_mapping_file, 'r') as f:
    for line in f:
        line = line.strip()
        if line and ':' in line:
            sheet_name, keyword = line.split(':', 1)
            script_sheet_map[sheet_name.strip()] = keyword.strip()

# --------------------- Load Total Transaction Mapping ---------------------
total_mapping_file = os.path.join(script_directory, "total_transaction_mapping.txt")
total_txn_map = {}
with open(total_mapping_file, 'r') as f:
    for line in f:
        line = line.strip()
        if line and ':' in line:
            script, txn_patterns = line.split(':', 1)
            total_txn_map[script.strip()] = [p.strip() for p in txn_patterns.split(',')]

# --------------------- Functions ---------------------
def process_excel_file(xls_file):
    xls = pd.ExcelFile(xls_file)
    df = pd.read_excel(xls, xls.sheet_names[0], header=None)

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
    sorted_data = selected_data.sort_values(by="Transaction Name")

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

def load_all_csvs_in_folder():
    csv_files = [f for f in os.listdir(raw_data_folder) if f.lower().endswith('.csv')]
    dataframes = {}
    for f in csv_files:
        try:
            df = pd.read_csv(os.path.join(raw_data_folder, f), header=None)
            if df.iloc[0].isnull().sum() == 0:
                df.columns = df.iloc[0]
                df = df[1:].reset_index(drop=True)
            else:
                df.columns = [f"Col_{i}" for i in range(df.shape[1])]
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

df1 = pd.read_excel(os.path.join(processed_folder, output_files[0]), sheet_name='Processed Data')
df2 = pd.read_excel(os.path.join(processed_folder, output_files[1]), sheet_name='Processed Data')

df1 = df1[['Transaction Name','Source File'] + columns_to_select[1:]]
df2 = df2[['Transaction Name','Source File'] + columns_to_select[1:]]

for df in [df1, df2]:
    for col in ['Pass','Fail','Stop','Average','90 Percent']:
        df[col] = safe_float_conversion(df[col])

merged = pd.merge(df1, df2, on='Transaction Name', how='outer', suffixes=('_Baseline','_NewCode'))

fname1_base = os.path.basename(output_files[0]).replace('Processed_','').replace('.xlsx','')
fname2_base = os.path.basename(output_files[1]).replace('Processed_','').replace('.xlsx','')
fname_row = ['' for _ in range(len(merged.columns))]
fname_row[merged.columns.get_loc('Source File_Baseline')] = fname1_base
fname_row[merged.columns.get_loc('Source File_NewCode')] = fname2_base
merged = pd.concat([pd.DataFrame([fname_row], columns=merged.columns), merged], ignore_index=True)
merged.iloc[1:, merged.columns.get_loc('Source File_Baseline')] = ''
merged.iloc[1:, merged.columns.get_loc('Source File_NewCode')] = ''

raw_csv_dict = load_all_csvs_in_folder()

# --------------------- Write Final Excel ---------------------
comparison_file = os.path.join(script_directory,'comparison.xlsx')
with pd.ExcelWriter(comparison_file, engine='xlsxwriter') as writer:

    wb = writer.book
    highlight_fmt = wb.add_format({'bg_color': '#FFF2CC'})
    green_fmt = wb.add_format({'bg_color': '#C6EFCE'})
    orange_fmt = wb.add_format({'bg_color': '#FFEB9C'})
    red_fmt = wb.add_format({'bg_color': '#FFC7CE'})
    link_fmt = wb.add_format({'font_color': 'blue','underline':1})

    # --------------------- 0️⃣ Index Sheet ---------------------
    index_df = pd.DataFrame({'Script Name': list(script_sheet_map.keys())})
    index_df.to_excel(writer, sheet_name='Index', index=False)
    ws_index = writer.sheets['Index']
    for row_idx, sheet_name in enumerate(index_df['Script Name'], start=1):
        ws_index.write_url(
            row_idx, 0, f"internal:'{sheet_name[:31]}'!A1",
            cell_format=link_fmt,
            string=sheet_name
        )
    autofit_worksheet_columns(ws_index, index_df)

    # --------------------- 1️⃣ One sheet per script ---------------------
    for sheet_name, keyword in script_sheet_map.items():
        keyword_lower = keyword.replace('%','').lower()
        df_script = merged[merged['Transaction Name'].astype(str).str.lower().str.contains(keyword_lower)].copy()

        # Safe diff calculations
        df_script['Avg_Diff'] = df_script['Average_NewCode'] - df_script['Average_Baseline']
        df_script['Avg Percent_Diff'] = (df_script['Average_NewCode'] - df_script['Average_Baseline']) / df_script['Average_Baseline'].replace({0: pd.NA})
        df_script['90 Percentile Diff'] = (df_script['90 Percent_NewCode'] - df_script['90 Percent_Baseline']) / df_script['90 Percent_Baseline'].replace({0: pd.NA})

        # Write sheet starting from row 1 to reserve row 0 for "Back to Index"
        df_script.to_excel(writer, sheet_name=sheet_name[:31], index=False, startrow=1)
        ws_script = writer.sheets[sheet_name[:31]]
        ws_script.freeze_panes(1,1)
        autofit_worksheet_columns(ws_script, df_script)

        # Dedicated "Back to Index" row at row 0
        ws_script.write_url(0, 0, "internal:'Index'!A1", cell_format=link_fmt, string="🔙 Back to Index")

        # Highlight mapped transactions
        if sheet_name in total_txn_map:
            mapped_patterns = total_txn_map[sheet_name]
            for row_idx, txn_name in enumerate(df_script['Transaction Name'], start=1):
                if any(p.lower() in str(txn_name).lower() for p in mapped_patterns):
                    for col_idx in range(len(df_script.columns)):
                        ws_script.write(row_idx, col_idx, df_script.iloc[row_idx-1, col_idx], highlight_fmt)

        # Conditional formatting
        for col_name in ['Avg_Diff','Avg Percent_Diff','90 Percentile Diff']:
            col_idx = df_script.columns.get_loc(col_name)
            ws_script.conditional_format(1, col_idx, len(df_script), col_idx,
                {'type':'cell','criteria':'<','value':0.1,'format':green_fmt})
            ws_script.conditional_format(1, col_idx, len(df_script), col_idx,
                {'type':'cell','criteria':'between','minimum':0.1,'maximum':0.25,'format':orange_fmt})
            ws_script.conditional_format(1, col_idx, len(df_script), col_idx,
                {'type':'cell','criteria':'>','value':0.25,'format':red_fmt})

        # Total (Mapped) row in Source File columns
        total_row = {'Transaction Name':'Total (Cases)'}
        if sheet_name in total_txn_map:
            mapped_patterns = total_txn_map[sheet_name]
            mask = df_script['Transaction Name'].astype(str).apply(lambda x: any(p.lower() in str(x).lower() for p in mapped_patterns))
            total_row['Source File_Baseline'] = df_script.loc[mask, 'Pass_Baseline'].sum()
            total_row['Source File_NewCode'] = df_script.loc[mask, 'Pass_NewCode'].sum()
        for col in df_script.columns:
            if col not in total_row:
                total_row[col] = ''
        start_row = len(df_script) + 1
        for col_idx, col_name in enumerate(df_script.columns):
            ws_script.write(start_row, col_idx, total_row[col_name], highlight_fmt)

    # --------------------- 2️⃣ Baseline_Info ---------------------
    excluded_dataframes[0].to_excel(writer, sheet_name='Baseline_Info', index=False)
    ws = writer.sheets['Baseline_Info']
    autofit_worksheet_columns(ws, excluded_dataframes[0])
    ws.set_row(0, None, None, {'hidden': True})

    # --------------------- 3️⃣ NewCode_Info ---------------------
    excluded_dataframes[1].to_excel(writer, sheet_name='NewCode_Info', index=False)
    ws = writer.sheets['NewCode_Info']
    autofit_worksheet_columns(ws, excluded_dataframes[1])
    ws.set_row(0, None, None, {'hidden': True})

    # --------------------- 4️⃣ Raw CSV sheets ---------------------
    for filename, df in raw_csv_dict.items():
        sheet_name_csv = filename[:31].replace(".", "_")
        df.to_excel(writer, sheet_name=sheet_name_csv, index=False)
        ws_raw = writer.sheets[sheet_name_csv]
        ws_raw.freeze_panes(1,0)
        autofit_worksheet_columns(ws_raw, df)

    # --------------------- 5️⃣ Instructions ---------------------
    instr_df = pd.DataFrame([["Instructions"],
                             ["This workbook contains script-level comparison sheets with mapped transactions highlighted."]])
    instr_df.to_excel(writer, sheet_name='Instructions', header=False, index=False)
    autofit_worksheet_columns(writer.sheets['Instructions'], instr_df)

print(f"Processed_*.xlsx files saved in '{processed_folder}'.")
print(f"Final comparison.xlsx saved in '{script_directory}'.")

import os
import pandas as pd

# --------------------- Setup ---------------------
script_directory = os.path.dirname(os.path.abspath(__file__))

raw_data_folder = os.path.join(script_directory, "rawData")
report_folder = os.path.join(script_directory, "Report")
processed_folder = os.path.join(script_directory, "Processed files")
os.makedirs(processed_folder, exist_ok=True)

columns_to_select = ["Transaction Name", "Average", "90 Percent", "Pass", "Fail", "Stop"]

# --------------------- Utility Functions ---------------------
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
        if ':' in line:
            sheet_name, keyword = line.strip().split(':', 1)
            script_sheet_map[sheet_name.strip()] = keyword.strip()

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
    sorted_data = selected_data.sort_values(by="Transaction Name").reset_index(drop=True)

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

# --------------------- Remove duplicates within baseline/newcode before merge ---------------------
df1 = df1.groupby('Transaction Name', as_index=False).agg({
    'Source File': 'first',
    'Average': 'mean',
    '90 Percent': 'mean',
    'Pass': 'sum',
    'Fail': 'sum',
    'Stop': 'sum'
})
df2 = df2.groupby('Transaction Name', as_index=False).agg({
    'Source File': 'first',
    'Average': 'mean',
    '90 Percent': 'mean',
    'Pass': 'sum',
    'Fail': 'sum',
    'Stop': 'sum'
})

raw_csv_dict = load_all_csvs_in_folder()

# --------------------- Write Final Excel ---------------------
comparison_file = os.path.join(script_directory,'comparison.xlsx')
with pd.ExcelWriter(comparison_file, engine='xlsxwriter') as writer:

    workbook = writer.book
    link_fmt = workbook.add_format({'font_color': 'blue','underline':1})
    all_sheet_names = []

    # --------------------- Index Sheet FIRST ---------------------
    index_df = pd.DataFrame({'Sheet Name': []})  # empty for now
    index_df.to_excel(writer, sheet_name='Index', index=False)
    ws_index = writer.sheets['Index']

    # --------------------- Script Sheets ---------------------
    for sheet_name, keyword in script_sheet_map.items():
        keyword_lower = keyword.replace('%','').lower()

        mask_baseline = df1['Transaction Name'].astype(str).str.lower().str.contains(keyword_lower)
        mask_newcode = df2['Transaction Name'].astype(str).str.lower().str.contains(keyword_lower)

        df_script = pd.merge(df1[mask_baseline], df2[mask_newcode],
                             on='Transaction Name', how='outer',
                             suffixes=('_Baseline','_NewCode'))

        df_script = df_script.drop_duplicates(subset=['Transaction Name']).reset_index(drop=True)
        df_script = df_script.sort_values(by='Transaction Name').reset_index(drop=True)

        # Calculated differences
        df_script['Avg_Diff'] = df_script['Average_NewCode'] - df_script['Average_Baseline']
        df_script['Avg Percent_Diff'] = (df_script['Average_NewCode'] - df_script['Average_Baseline']) / df_script['Average_Baseline'].replace({0: pd.NA})
        df_script['90 Percentile Diff'] = (df_script['90 Percent_NewCode'] - df_script['90 Percent_Baseline']) / df_script['90 Percent_Baseline'].replace({0: pd.NA})

        df_script_to_write = df_script.drop(columns=['Avg Percent_Diff'])
        df_script_to_write.to_excel(writer, sheet_name=sheet_name[:31], index=False, startrow=1)
        ws_script = writer.sheets[sheet_name[:31]]
        ws_script.freeze_panes(1,1)
        autofit_worksheet_columns(ws_script, df_script_to_write)

        ws_script.write_url(0, 0, "internal:'Index'!A1", cell_format=link_fmt, string="Back to Index")

        # Conditional formatting
        green_format = workbook.add_format({'bg_color': '#EBF1DE'})
        orange_format = workbook.add_format({'bg_color': '#FFA500'})
        red_format = workbook.add_format({'bg_color': '#FF5D5D'})

        avg_diff_col = df_script_to_write.columns.get_loc('Avg_Diff')
        ninty_perc_diff_col = df_script_to_write.columns.get_loc('90 Percentile Diff')

        for row in range(2, 2 + len(df_script_to_write)):
            ws_script.conditional_format(row, avg_diff_col, row, avg_diff_col, {'type': 'cell', 'criteria': '<', 'value': 0, 'format': green_format})
            ws_script.conditional_format(row, avg_diff_col, row, avg_diff_col, {'type': 'formula', 'criteria': f'=ABS(${"{:c}".format(65 + avg_diff_col)}${row+1})<0.1', 'format': green_format})
            ws_script.conditional_format(row, avg_diff_col, row, avg_diff_col, {'type': 'formula', 'criteria': f'=AND(ABS(${"{:c}".format(65 + avg_diff_col)}${row+1})>=0.1, ABS(${"{:c}".format(65 + avg_diff_col)}${row+1})<=0.25)', 'format': orange_format})
            ws_script.conditional_format(row, avg_diff_col, row, avg_diff_col, {'type': 'formula', 'criteria': f'=ABS(${"{:c}".format(65 + avg_diff_col)}${row+1})>0.25', 'format': red_format})

            ws_script.conditional_format(row, ninty_perc_diff_col, row, ninty_perc_diff_col, {'type': 'cell', 'criteria': '<', 'value': 0, 'format': green_format})
            ws_script.conditional_format(row, ninty_perc_diff_col, row, ninty_perc_diff_col, {'type': 'formula', 'criteria': f'=ABS(${"{:c}".format(65 + ninty_perc_diff_col)}${row+1})<0.1', 'format': green_format})
            ws_script.conditional_format(row, ninty_perc_diff_col, row, ninty_perc_diff_col, {'type': 'formula', 'criteria': f'=AND(ABS(${"{:c}".format(65 + ninty_perc_diff_col)}${row+1})>=0.1, ABS(${"{:c}".format(65 + ninty_perc_diff_col)}${row+1})<=0.25)', 'format': orange_format})
            ws_script.conditional_format(row, ninty_perc_diff_col, row, ninty_perc_diff_col, {'type': 'formula', 'criteria': f'=ABS(${"{:c}".format(65 + ninty_perc_diff_col)}${row+1})>0.25', 'format': red_format})

        all_sheet_names.append(sheet_name[:31])

    # --------------------- Baseline & NewCode Info ---------------------
    excluded_dataframes[0].to_excel(writer, sheet_name='Baseline_Info', index=False)
    ws_baseline = writer.sheets['Baseline_Info']
    ws_baseline.write_url(0, 0, "internal:'Index'!A1", cell_format=link_fmt, string="Back to Index")
    autofit_worksheet_columns(ws_baseline, excluded_dataframes[0])
    all_sheet_names.append('Baseline_Info')

    excluded_dataframes[1].to_excel(writer, sheet_name='NewCode_Info', index=False)
    ws_newcode = writer.sheets['NewCode_Info']
    ws_newcode.write_url(0, 0, "internal:'Index'!A1", cell_format=link_fmt, string="Back to Index")
    autofit_worksheet_columns(ws_newcode, excluded_dataframes[1])
    all_sheet_names.append('NewCode_Info')

    # --------------------- Raw CSV Sheets ---------------------
    for filename, df in raw_csv_dict.items():
        sheet_name_csv = filename[:31].replace(".", "_")
        df.to_excel(writer, sheet_name=sheet_name_csv, index=False)
        ws_raw = writer.sheets[sheet_name_csv]
        ws_raw.write_url(0, 0, "internal:'Index'!A1", cell_format=link_fmt, string="Back to Index")
        ws_raw.freeze_panes(1,0)
        autofit_worksheet_columns(ws_raw, df)
        all_sheet_names.append(sheet_name_csv)

    # --------------------- Update Index Sheet with all hyperlinks ---------------------
    for row_idx, sheet_name in enumerate(all_sheet_names, start=1):
        ws_index.write_url(row_idx, 0, f"internal:'{sheet_name}'!A1", cell_format=link_fmt, string=sheet_name)

print("comparison.xlsx generated successfully.")

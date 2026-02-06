import os
import pandas as pd
import numpy as np
from datetime import datetime
import json
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders
import logging
from pathlib import Path

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('excel_analyzer.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Set the directory path to the current script location
script_directory = os.path.dirname(os.path.abspath(__file__))

class ExcelAnalyzer:
    def __init__(self, config_file='config.json'):
        """Initialize the analyzer with configuration."""
        self.script_directory = script_directory
        self.config = self.load_config(config_file)
        self.columns_to_select = self.config.get('columns_to_select', 
            ["Transaction Name", "Average", "90 Percent", "Pass", "Fail", "Stop"])
        self.transaction_patterns = self.config.get('transaction_patterns', 
            ["Transaction_1", "Transaction_2", "Transaction_3", "Transaction_4"])
        
    def load_config(self, config_file):
        """Load configuration from JSON file."""
        config_path = os.path.join(self.script_directory, config_file)
        
        # Default configuration
        default_config = {
            "columns_to_select": ["Transaction Name", "Average", "90 Percent", "Pass", "Fail", "Stop"],
            "transaction_patterns": ["Transaction_1", "Transaction_2", "Transaction_3", "Transaction_4"],
            "email_enabled": False,
            "email_settings": {
                "smtp_server": "smtp.gmail.com",
                "smtp_port": 587,
                "sender_email": "",
                "sender_password": "",
                "recipient_emails": []
            },
            "analysis_settings": {
                "generate_charts": True,
                "performance_threshold_warning": 0.1,
                "performance_threshold_critical": 0.25,
                "include_statistics": True
            },
            "file_settings": {
                "number_of_files_to_process": 2,
                "output_filename": "comparison.xlsx"
            }
        }
        
        if os.path.exists(config_path):
            try:
                with open(config_path, 'r') as f:
                    user_config = json.load(f)
                    default_config.update(user_config)
                    logger.info(f"Configuration loaded from {config_file}")
            except Exception as e:
                logger.warning(f"Error loading config file: {e}. Using defaults.")
        else:
            # Create default config file
            with open(config_path, 'w') as f:
                json.dump(default_config, f, indent=4)
            logger.info(f"Created default configuration file: {config_file}")
        
        return default_config

    def is_safe_file_path(self, file_path):
        """Check if the file path is within the script's directory."""
        try:
            return os.path.commonpath([self.script_directory, file_path]) == self.script_directory
        except ValueError:
            return False

    def find_excel_files(self, num_files=2):
        """Find Excel files in the directory and return the oldest ones by modification time."""
        try:
            xls_files = sorted(
                [
                    os.path.join(self.script_directory, filename)
                    for filename in os.listdir(self.script_directory)
                    if (filename.endswith('.xls') or filename.endswith('.xlsx')) 
                    and is_safe_file_path(os.path.join(self.script_directory, filename))
                    and not filename.startswith('Processed_')
                    and not filename.startswith('comparison')
                ],
                key=os.path.getmtime
            )
            
            logger.info(f"Found {len(xls_files)} Excel files")
            return xls_files[:num_files]
        except Exception as e:
            logger.error(f"Error finding Excel files: {e}")
            return []

    def process_excel_file(self, xls_file):
        """Process a single Excel file and extract relevant data."""
        try:
            if not self.is_safe_file_path(xls_file):
                raise ValueError(f"Unsafe file path detected: {xls_file}")
            
            logger.info(f"Processing file: {os.path.basename(xls_file)}")
            
            # Load Excel file and get the first sheet
            xls = pd.ExcelFile(xls_file)
            df = pd.read_excel(xls, xls.sheet_names[0], header=None)
            start_idx = None
            end_idx = None

            # Find indices for "Transaction Name" and "Codes"
            for index, row in df.iterrows():
                if row.astype(str).str.contains('Transaction Name', na=False).any():
                    start_idx = index
                    break

            for index, row in df.iterrows():
                if row.astype(str).str.contains('Codes', na=False).any():
                    end_idx = index
                    break

            # If valid indices, process data
            if start_idx is not None and (end_idx is not None or (end_idx is None and start_idx < len(df))):
                end_idx = end_idx if end_idx is not None else len(df)
                filtered_data = df.iloc[start_idx:end_idx]
                excluded_data = df.drop(filtered_data.index)

                # Set header from the identified start row and process columns
                header_row = filtered_data.iloc[0]
                filtered_data.columns = header_row
                selected_data = filtered_data[self.columns_to_select].iloc[1:]
                sorted_data = selected_data.sort_values(by="Transaction Name")
                sorted_data['Source File'] = os.path.basename(xls_file)

                # Define output filename
                input_filename = os.path.basename(xls_file)
                name, ext = os.path.splitext(input_filename)
                output_filename = f"Processed_{name}.xlsx"
                output_path = os.path.join(self.script_directory, output_filename)

                if not self.is_safe_file_path(output_path):
                    raise ValueError(f"Unsafe output path detected: {output_path}")

                # Save filtered and excluded data
                with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
                    sorted_data.to_excel(writer, sheet_name='Processed Data', index=False)
                    excluded_data.to_excel(writer, sheet_name='Excluded Data', index=False)

                logger.info(f"Successfully processed: {output_filename}")
                return sorted_data, excluded_data, output_filename

            logger.warning(f"Could not find valid data structure in {xls_file}")
            return None, None, None
            
        except Exception as e:
            logger.error(f"Error processing {xls_file}: {e}")
            return None, None, None

    def calculate_statistics(self, df, column_name):
        """Calculate statistical measures for a given column."""
        try:
            data = pd.to_numeric(df[column_name], errors='coerce').dropna()
            
            if len(data) == 0:
                return {}
            
            stats = {
                'mean': data.mean(),
                'median': data.median(),
                'std': data.std(),
                'min': data.min(),
                'max': data.max(),
                'q25': data.quantile(0.25),
                'q75': data.quantile(0.75),
                'count': len(data)
            }
            
            return stats
        except Exception as e:
            logger.error(f"Error calculating statistics for {column_name}: {e}")
            return {}

    def matches_any_pattern(self, name, patterns):
        """Check if names match patterns."""
        return any(pattern.lower() in name.lower() for pattern in patterns)

    def create_comparison_analysis(self, df1, df2, output_files, excluded_dataframes):
        """Create comprehensive comparison analysis with charts and statistics."""
        try:
            logger.info("Creating comparison analysis...")
            
            # Assign filename bases
            filename1_base = os.path.basename(output_files[0]).replace('Processed_', '').replace('.xlsx', '')
            filename2_base = os.path.basename(output_files[1]).replace('Processed_', '').replace('.xlsx', '')

            # Format data for comparison
            df1 = df1[['Transaction Name', 'Source File'] + self.columns_to_select[1:]]
            df2 = df2[['Transaction Name', 'Source File'] + self.columns_to_select[1:]]

            # Ensure 'Transaction Name' is a string
            df1['Transaction Name'] = df1['Transaction Name'].astype(str).fillna('')
            df2['Transaction Name'] = df2['Transaction Name'].astype(str).fillna('')

            # Filter based on patterns
            filtered_df1 = df1[df1['Transaction Name'].apply(
                lambda x: self.matches_any_pattern(x, self.transaction_patterns))].copy()
            filtered_df2 = df2[df2['Transaction Name'].apply(
                lambda x: self.matches_any_pattern(x, self.transaction_patterns))].copy()

            # Convert numeric columns
            for df in [filtered_df1, filtered_df2]:
                for col in self.columns_to_select[1:]:
                    df.loc[:, col] = df[col].astype(str).str.replace(',', '').astype(float)

            # Create pass count summary
            pass_summary = self.create_pass_summary(filtered_df1, filtered_df2)
            
            # Create merged comparison data
            merged_data = self.create_merged_comparison(df1, df2, filename1_base, filename2_base)
            
            # Calculate statistics if enabled
            statistics_df = None
            if self.config['analysis_settings']['include_statistics']:
                statistics_df = self.create_statistics_summary(filtered_df1, filtered_df2)
            
            # Create performance summary
            performance_summary = self.create_performance_summary(merged_data)
            
            # Write to Excel with charts
            output_file = os.path.join(self.script_directory, 
                                      self.config['file_settings']['output_filename'])
            
            if not self.is_safe_file_path(output_file):
                raise ValueError(f"Unsafe output path detected: {output_file}")
            
            self.write_excel_with_charts(
                output_file, 
                merged_data, 
                excluded_dataframes,
                pass_summary,
                statistics_df,
                performance_summary,
                filename1_base,
                filename2_base
            )
            
            logger.info(f"Analysis complete. Output saved to {output_file}")
            return output_file
            
        except Exception as e:
            logger.error(f"Error creating comparison analysis: {e}")
            raise

    def create_pass_summary(self, df1, df2):
        """Create pass count summary."""
        pass_summary_df1 = df1.groupby('Transaction Name')['Pass'].sum().reset_index(name='Pass Count 1')
        pass_summary_df2 = df2.groupby('Transaction Name')['Pass'].sum().reset_index(name='Pass Count 2')

        total_pass_df1 = pass_summary_df1['Pass Count 1'].sum()
        total_pass_df2 = pass_summary_df2['Pass Count 2'].sum()

        total_row_df1 = pd.DataFrame({'Transaction Name': ['Total'], 'Pass Count 1': [total_pass_df1]})
        total_row_df2 = pd.DataFrame({'Transaction Name': ['Total'], 'Pass Count 2': [total_pass_df2]})

        pass_summary_df1 = pd.concat([pass_summary_df1, total_row_df1], ignore_index=True)
        pass_summary_df2 = pd.concat([pass_summary_df2, total_row_df2], ignore_index=True)

        final_pass_summary = pd.merge(pass_summary_df1, pass_summary_df2, on="Transaction Name", how="outer")

        customized_columns = {
            'Transaction Name': 'Transaction ID',
            'Pass Count 1': 'Pass Count (Oldest)',
            'Pass Count 2': 'Pass Count (Newest)',
        }
        final_pass_summary.rename(columns=customized_columns, inplace=True)
        
        return final_pass_summary

    def create_merged_comparison(self, df1, df2, filename1_base, filename2_base):
        """Create merged comparison data with calculated differences."""
        merged_data = pd.merge(df1, df2, on="Transaction Name", how="outer", 
                              suffixes=('_Baseline', '_NewCode'))
        
        merged_data['Avg_Diff'] = merged_data['Average_NewCode'] - merged_data['Average_Baseline']
        merged_data['Avg Percent_Diff'] = (merged_data['Average_NewCode'] - merged_data['Average_Baseline']) / merged_data['Average_Baseline']
        merged_data['90 Percentile Diff'] = (merged_data['90 Percent_NewCode'] - merged_data['90 Percent_Baseline']) / merged_data['90 Percent_Baseline']

        comparison_customized_columns = {
            'Average_Baseline': 'Average (Baseline)',
            'Average_NewCode': 'Average (New Code)',
            '90 Percent_Baseline': '90 Percent (Baseline)',
            '90 Percent_NewCode': '90 Percent (New Code)',
            'Pass_Baseline': 'Pass (Baseline)',
            'Pass_NewCode': 'Pass (New Code)',
            'Fail_Baseline': 'Fail (Baseline)',
            'Fail_NewCode': 'Fail (New Code)',
            'Stop_Baseline': 'Stop (Baseline)',
            'Stop_NewCode': 'Stop (New Code)'
        }
        merged_data.rename(columns=comparison_customized_columns, inplace=True)

        # Add filename row
        filename_row = ["" for _ in range(len(merged_data.columns))]
        filename_row[merged_data.columns.get_loc('Source File_Baseline')] = filename1_base
        filename_row[merged_data.columns.get_loc('Source File_NewCode')] = filename2_base

        filename_df = pd.DataFrame([filename_row], columns=merged_data.columns)
        merged_data = pd.concat([filename_df, merged_data], ignore_index=True, axis=0)
        merged_data.iloc[1:, merged_data.columns.get_loc('Source File_Baseline')] = ""
        merged_data.iloc[1:, merged_data.columns.get_loc('Source File_NewCode')] = ""
        
        return merged_data

    def create_statistics_summary(self, df1, df2):
        """Create statistical summary for key metrics."""
        stats_data = []
        
        for metric in ['Average', '90 Percent', 'Pass', 'Fail', 'Stop']:
            stats1 = self.calculate_statistics(df1, metric)
            stats2 = self.calculate_statistics(df2, metric)
            
            if stats1 and stats2:
                stats_data.append({
                    'Metric': metric,
                    'Baseline Mean': stats1.get('mean', 0),
                    'Baseline Median': stats1.get('median', 0),
                    'Baseline Std': stats1.get('std', 0),
                    'NewCode Mean': stats2.get('mean', 0),
                    'NewCode Median': stats2.get('median', 0),
                    'NewCode Std': stats2.get('std', 0),
                    'Mean Difference': stats2.get('mean', 0) - stats1.get('mean', 0),
                    'Mean % Change': ((stats2.get('mean', 0) - stats1.get('mean', 0)) / stats1.get('mean', 1)) * 100
                })
        
        return pd.DataFrame(stats_data)

    def create_performance_summary(self, merged_data):
        """Create performance summary with categorization."""
        # Skip the first row (filename row)
        data = merged_data.iloc[1:].copy()
        
        warning_threshold = self.config['analysis_settings']['performance_threshold_warning']
        critical_threshold = self.config['analysis_settings']['performance_threshold_critical']
        
        summary_data = []
        
        # Count performance improvements and regressions
        avg_diff = pd.to_numeric(data['Avg_Diff'], errors='coerce').dropna()
        percent_diff = pd.to_numeric(data['Avg Percent_Diff'], errors='coerce').dropna()
        
        improved = len(avg_diff[avg_diff < 0])
        degraded = len(avg_diff[avg_diff > 0])
        
        warning_count = len(percent_diff[(percent_diff.abs() >= warning_threshold) & 
                                        (percent_diff.abs() < critical_threshold)])
        critical_count = len(percent_diff[percent_diff.abs() >= critical_threshold])
        
        summary_data.append({
            'Category': 'Performance Improved',
            'Count': improved,
            'Percentage': f"{(improved/len(avg_diff)*100):.1f}%" if len(avg_diff) > 0 else "0%"
        })
        
        summary_data.append({
            'Category': 'Performance Degraded',
            'Count': degraded,
            'Percentage': f"{(degraded/len(avg_diff)*100):.1f}%" if len(avg_diff) > 0 else "0%"
        })
        
        summary_data.append({
            'Category': 'Warning Level Changes',
            'Count': warning_count,
            'Percentage': f"{(warning_count/len(percent_diff)*100):.1f}%" if len(percent_diff) > 0 else "0%"
        })
        
        summary_data.append({
            'Category': 'Critical Level Changes',
            'Count': critical_count,
            'Percentage': f"{(critical_count/len(percent_diff)*100):.1f}%" if len(percent_diff) > 0 else "0%"
        })
        
        return pd.DataFrame(summary_data)

    def write_excel_with_charts(self, output_file, merged_data, excluded_dataframes, 
                                pass_summary, statistics_df, performance_summary,
                                filename1_base, filename2_base):
        """Write Excel file with charts and formatting."""
        with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
            workbook = writer.book
            
            # Write all sheets
            merged_data.to_excel(writer, sheet_name='Comparison', index=False)
            excluded_dataframes[0].to_excel(writer, sheet_name='Baseline_Info', index=False)
            excluded_dataframes[1].to_excel(writer, sheet_name='NewCode_Info', index=False)
            pass_summary.to_excel(writer, sheet_name='Total_Case_Created', index=False)
            
            if statistics_df is not None:
                statistics_df.to_excel(writer, sheet_name='Statistics', index=False)
            
            performance_summary.to_excel(writer, sheet_name='Performance_Summary', index=False)
            
            # Write instructions
            self.write_instructions_sheet(writer)
            
            # Apply formatting to Comparison sheet
            self.format_comparison_sheet(writer, merged_data, workbook)
            
            # Add charts if enabled
            if self.config['analysis_settings']['generate_charts']:
                self.add_charts(writer, workbook, pass_summary, statistics_df, 
                              performance_summary, filename1_base, filename2_base)

    def format_comparison_sheet(self, writer, merged_data, workbook):
        """Apply conditional formatting to the comparison sheet."""
        worksheet = writer.sheets['Comparison']
        worksheet.freeze_panes(2, 1)

        # Define formats
        green_format = workbook.add_format({'bg_color': '#EBF1DE'})
        orange_format = workbook.add_format({'bg_color': '#FFA500'})
        red_format = workbook.add_format({'bg_color': '#FF5D5D'})

        # Get column indices
        avg_diff_col = merged_data.columns.get_loc('Avg_Diff')
        percent_diff_col = merged_data.columns.get_loc('Avg Percent_Diff')
        ninty_perc_diff_col = merged_data.columns.get_loc('90 Percentile Diff')

        warning_threshold = self.config['analysis_settings']['performance_threshold_warning']
        critical_threshold = self.config['analysis_settings']['performance_threshold_critical']

        # Apply conditional formatting
        for row in range(2, len(merged_data)):
            # Green for improvements (negative values)
            for col in [avg_diff_col, percent_diff_col, ninty_perc_diff_col]:
                worksheet.conditional_format(row, col, row, col, {
                    'type': 'cell',
                    'criteria': '<',
                    'value': 0,
                    'format': green_format
                })
            
            # Apply threshold-based formatting
            for col in [avg_diff_col, percent_diff_col, ninty_perc_diff_col]:
                col_letter = chr(65 + col)
                
                # Green: below warning threshold
                worksheet.conditional_format(row, col, row, col, {
                    'type': 'formula',
                    'criteria': f'=ABS(${col_letter}${row+1})<{warning_threshold}',
                    'format': green_format
                })
                
                # Orange: warning level
                worksheet.conditional_format(row, col, row, col, {
                    'type': 'formula',
                    'criteria': f'=AND(ABS(${col_letter}${row+1})>={warning_threshold}, ABS(${col_letter}${row+1})<{critical_threshold})',
                    'format': orange_format
                })
                
                # Red: critical level
                worksheet.conditional_format(row, col, row, col, {
                    'type': 'formula',
                    'criteria': f'=ABS(${col_letter}${row+1})>={critical_threshold}',
                    'format': red_format
                })

    def add_charts(self, writer, workbook, pass_summary, statistics_df, 
                  performance_summary, filename1_base, filename2_base):
        """Add charts to the Excel workbook."""
        
        # Chart 1: Pass Count Comparison (on Total_Case_Created sheet)
        self.add_pass_count_chart(writer, workbook, pass_summary, filename1_base, filename2_base)
        
        # Chart 2: Performance Summary (on Performance_Summary sheet)
        self.add_performance_chart(writer, workbook, performance_summary)
        
        # Chart 3: Statistics Comparison (if statistics are included)
        if statistics_df is not None:
            self.add_statistics_chart(writer, workbook, statistics_df)

    def add_pass_count_chart(self, writer, workbook, pass_summary, filename1_base, filename2_base):
        """Add bar chart comparing pass counts."""
        try:
            worksheet = writer.sheets['Total_Case_Created']
            
            chart = workbook.add_chart({'type': 'column'})
            
            # Configure the chart
            num_rows = len(pass_summary) - 1  # Exclude total row for chart
            
            chart.add_series({
                'name': f'=Total_Case_Created!$B$1',
                'categories': f'=Total_Case_Created!$A$2:$A${num_rows+1}',
                'values': f'=Total_Case_Created!$B$2:$B${num_rows+1}',
                'fill': {'color': '#4472C4'}
            })
            
            chart.add_series({
                'name': f'=Total_Case_Created!$C$1',
                'categories': f'=Total_Case_Created!$A$2:$A${num_rows+1}',
                'values': f'=Total_Case_Created!$C$2:$C${num_rows+1}',
                'fill': {'color': '#ED7D31'}
            })
            
            chart.set_title({'name': 'Pass Count Comparison by Transaction'})
            chart.set_x_axis({'name': 'Transaction'})
            chart.set_y_axis({'name': 'Pass Count'})
            chart.set_style(11)
            
            worksheet.insert_chart('E2', chart, {'x_scale': 2, 'y_scale': 1.5})
            
            logger.info("Added pass count comparison chart")
        except Exception as e:
            logger.error(f"Error adding pass count chart: {e}")

    def add_performance_chart(self, writer, workbook, performance_summary):
        """Add pie chart showing performance distribution."""
        try:
            worksheet = writer.sheets['Performance_Summary']
            
            chart = workbook.add_chart({'type': 'pie'})
            
            chart.add_series({
                'name': 'Performance Distribution',
                'categories': f'=Performance_Summary!$A$2:$A${len(performance_summary)+1}',
                'values': f'=Performance_Summary!$B$2:$B${len(performance_summary)+1}',
                'data_labels': {'percentage': True, 'category': True},
            })
            
            chart.set_title({'name': 'Performance Change Distribution'})
            chart.set_style(10)
            
            worksheet.insert_chart('E2', chart, {'x_scale': 1.5, 'y_scale': 1.5})
            
            logger.info("Added performance distribution chart")
        except Exception as e:
            logger.error(f"Error adding performance chart: {e}")

    def add_statistics_chart(self, writer, workbook, statistics_df):
        """Add chart comparing statistical metrics."""
        try:
            worksheet = writer.sheets['Statistics']
            
            chart = workbook.add_chart({'type': 'bar'})
            
            num_rows = len(statistics_df)
            
            chart.add_series({
                'name': 'Baseline Mean',
                'categories': f'=Statistics!$A$2:$A${num_rows+1}',
                'values': f'=Statistics!$B$2:$B${num_rows+1}',
            })
            
            chart.add_series({
                'name': 'NewCode Mean',
                'categories': f'=Statistics!$A$2:$A${num_rows+1}',
                'values': f'=Statistics!$E$2:$E${num_rows+1}',
            })
            
            chart.set_title({'name': 'Mean Comparison by Metric'})
            chart.set_x_axis({'name': 'Value'})
            chart.set_y_axis({'name': 'Metric'})
            chart.set_style(11)
            
            worksheet.insert_chart('K2', chart, {'x_scale': 2, 'y_scale': 1.5})
            
            logger.info("Added statistics comparison chart")
        except Exception as e:
            logger.error(f"Error adding statistics chart: {e}")

    def write_instructions_sheet(self, writer):
        """Write instructions and documentation sheet."""
        instructions_content = [
            ["Enhanced Excel Analyzer - Instructions"],
            [""],
            ["This workbook contains comprehensive performance analysis comparing two test runs."],
            [""],
            ["Sheet Descriptions:"],
            ["- 'Comparison': Main comparison data with conditional formatting"],
            ["  * Green: Performance improvement or change < 10%"],
            ["  * Orange: Performance change between 10-25% (warning level)"],
            ["  * Red: Performance change > 25% (critical level)"],
            [""],
            ["- 'Total_Case_Created': Pass count summary with comparison chart"],
            [""],
            ["- 'Performance_Summary': High-level performance statistics and distribution"],
            [""],
            ["- 'Statistics': Detailed statistical analysis of key metrics"],
            ["  * Includes mean, median, standard deviation"],
            ["  * Shows percentage changes between runs"],
            [""],
            ["- 'Baseline_Info' & 'NewCode_Info': Raw excluded data from each file"],
            [""],
            ["Security Features:"],
            ["- All file operations restricted to script directory"],
            ["- Protection against directory traversal attacks"],
            ["- Safe file path validation"],
            [""],
            ["Configuration:"],
            ["- Modify config.json to customize analysis parameters"],
            ["- Set thresholds, email settings, and transaction patterns"],
            [""],
            [f"Report Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"],
            [""],
            ["For questions or issues, check the log file: excel_analyzer.log"]
        ]
        
        instructions_df = pd.DataFrame(instructions_content)
        instructions_df.to_excel(writer, sheet_name='Instructions', header=False, index=False)

    def send_email_report(self, attachment_path):
        """Send email report with the analysis file attached."""
        if not self.config['email_enabled']:
            logger.info("Email reporting is disabled in configuration")
            return
        
        try:
            email_settings = self.config['email_settings']
            
            # Create message
            msg = MIMEMultipart()
            msg['From'] = email_settings['sender_email']
            msg['To'] = ', '.join(email_settings['recipient_emails'])
            msg['Subject'] = f"Excel Analysis Report - {datetime.now().strftime('%Y-%m-%d')}"
            
            # Email body
            body = f"""
            Excel Analysis Report
            
            Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
            
            This automated report contains the comparison analysis of performance test results.
            
            Please find the detailed Excel report attached.
            
            Summary:
            - Files processed successfully
            - Comparison analysis completed
            - Charts and statistics included
            
            Best regards,
            Automated Excel Analyzer
            """
            
            msg.attach(MIMEText(body, 'plain'))
            
            # Attach file
            with open(attachment_path, 'rb') as attachment:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(attachment.read())
            
            encoders.encode_base64(part)
            part.add_header(
                'Content-Disposition',
                f'attachment; filename= {os.path.basename(attachment_path)}',
            )
            
            msg.attach(part)
            
            # Send email
            server = smtplib.SMTP(email_settings['smtp_server'], email_settings['smtp_port'])
            server.starttls()
            server.login(email_settings['sender_email'], email_settings['sender_password'])
            text = msg.as_string()
            server.sendmail(email_settings['sender_email'], 
                          email_settings['recipient_emails'], 
                          text)
            server.quit()
            
            logger.info(f"Email sent successfully to {email_settings['recipient_emails']}")
            
        except Exception as e:
            logger.error(f"Error sending email: {e}")

    def run_analysis(self):
        """Main method to run the complete analysis."""
        try:
            logger.info("=" * 50)
            logger.info("Starting Excel Analysis")
            logger.info("=" * 50)
            
            # Find Excel files
            num_files = self.config['file_settings']['number_of_files_to_process']
            xls_files = self.find_excel_files(num_files)
            
            if len(xls_files) < 2:
                logger.error(f"Found only {len(xls_files)} Excel files. Need at least 2 files.")
                print("Error: Less than two Excel files found in the directory.")
                return None
            
            # Process files
            output_files = []
            excluded_dataframes = []
            
            for file in xls_files:
                processed_data, excluded_data, output_filename = self.process_excel_file(file)
                if output_filename:
                    output_files.append(output_filename)
                    excluded_dataframes.append(excluded_data)
            
            if len(output_files) < 2:
                logger.error("Failed to process enough files for comparison")
                return None
            
            # Load processed data
            df1_path = os.path.join(self.script_directory, output_files[0])
            df2_path = os.path.join(self.script_directory, output_files[1])
            
            df1 = pd.read_excel(df1_path, sheet_name='Processed Data')
            df2 = pd.read_excel(df2_path, sheet_name='Processed Data')
            
            # Create comparison analysis
            output_file = self.create_comparison_analysis(df1, df2, output_files, excluded_dataframes)
            
            # Send email if enabled
            if self.config['email_enabled']:
                self.send_email_report(output_file)
            
            logger.info("=" * 50)
            logger.info("Analysis Complete!")
            logger.info(f"Output file: {output_file}")
            logger.info("=" * 50)
            
            print(f"\n✓ Analysis complete! Output saved to: {output_file}")
            
            return output_file
            
        except Exception as e:
            logger.error(f"Fatal error during analysis: {e}", exc_info=True)
            print(f"\n✗ Error: {e}")
            return None


def is_safe_file_path(file_path):
    """Global helper function for safety checks."""
    try:
        return os.path.commonpath([script_directory, file_path]) == script_directory
    except ValueError:
        return False


# Main execution
if __name__ == "__main__":
    print("Enhanced Excel Analyzer v2.0")
    print("=" * 50)
    
    analyzer = ExcelAnalyzer()
    result = analyzer.run_analysis()
    
    if result:
        print("\nAnalysis completed successfully!")
        print(f"Check the log file for details: excel_analyzer.log")
    else:
        print("\nAnalysis failed. Check the log file for details.")
import os
import re
import csv
from pathlib import Path
import logging
from datetime import datetime
import pdfplumber
import streamlit as st
import pandas as pd
import tempfile
import base64

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PDFTextExtractor:
    """Extract PDF text using pdfplumber"""
    
    def extract_text_from_pdf(self, pdf_path):
        """Reliably extract text using pdfplumber"""
        text = ""
        try:
            with pdfplumber.open(pdf_path) as pdf:
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text += page_text + "\n"

            if not text.strip():
                return "Failed to extract text"

            return text

        except Exception as e:
            logger.error(f"Failed to extract PDF text: {str(e)}")
            return "Failed to extract text"

class DataMapper:
    """Data mapper - Convert first table to second table"""
    
    def __init__(self):
        self.target_columns = [
            "Picture", "Production Type", "Claim Type", "Vendor", "Claim No.", 
            "Claim Date", "Inspection Date", "Customer", "Dept.", "FID", 
            "TEAM", "QC Trip Leader", "Style NO.", "Order No.", "Article No.", 
            "Relevant shipped Qty", "Quality Digit (Market)", "Defect Code", 
            "Claim Reason", "QC Responsibility", "Claim Status", "Validate Month", 
            "Claim shipped Qty", "Random check in customer warehouse", "Re-check in warehouse"
        ]
    
    def map_to_target_format(self, source_df):
        """Map source data to target format"""
        if source_df.empty:
            return pd.DataFrame(columns=self.target_columns)
        
        mapped_data = []
        
        for index, row in source_df.iterrows():
            mapped_row = self.process_single_row(row)
            mapped_data.append(mapped_row)
        
        result_df = pd.DataFrame(mapped_data, columns=self.target_columns)
        
        # Ensure Random check in customer warehouse field is text format
        if "Random check in customer warehouse" in result_df.columns:
            result_df["Random check in customer warehouse"] = result_df["Random check in customer warehouse"].astype(str)
        
        return result_df
    
    def process_single_row(self, source_row):
        """Process single row data mapping"""
        mapped_row = {col: "" for col in self.target_columns}
        
        # 1. Claim Type logic (based on VBA code logic)
        mapped_row["Claim Type"] = self.get_claim_type(source_row)
        
        # 2. Claim Status fixed as Failure
        mapped_row["Claim Status"] = "Failure"
        
        # 3. Basic field mapping
        field_mappings = {
            "Vendor": "Supplier Name",
            "Claim No.": "Claim no", 
            "Customer": "customer_name",
            "Dept.": "Dept.",
            "Style NO.": "Style No",
            "Order No.": "Order No",
            "Article No.": "Item No",
            "Relevant shipped Qty": "Delivered quantity",
            "Claim Reason": "Description of faults",
            "Claim Date": "Date of decision"
        }
        
        for target_field, source_field in field_mappings.items():
            mapped_row[target_field] = self.safe_extract_value(source_row, source_field)
        
        # 4. Combined field: Random check in customer warehouse
        mapped_row["Random check in customer warehouse"] = self.combine_faulty_random(source_row)
        
        # 5. Validate Month left empty
        mapped_row["Validate Month"] = ""
        
        # 6. Other fields remain empty (Picture, Production Type, Inspection Date, FID, TEAM, 
        #    QC Trip Leader, Quality Digit, Defect Code, QC Responsibility, Claim shipped Qty, Re-check in warehouse)
        
        return mapped_row
    
    def get_claim_type(self, row):
        """Determine Claim Type based on Decision column (based on VBA logic)"""
        decision_value = self.safe_extract_value(row, "Decision")
        
        if not decision_value:
            return "Claim"  # Default value
        
        # Check if contains "Not extracted"
        if "Not extracted" in decision_value:
            return ""
        
        # Determine based on specific values
        if decision_value in ["QD45 (Q)", "Q"]:
            return "Complaint"
        else:
            return "Claim"
    
    def combine_faulty_random(self, row):
        """Combine Faulty pcs and Random quantity fields"""
        faulty_pcs = self.safe_extract_value(row, "Faulty pcs")
        random_qty = self.safe_extract_value(row, "Random quantity")
        
        # Only combine when both values exist and are not empty
        if faulty_pcs and random_qty and faulty_pcs != "Not extracted" and random_qty != "Not extracted":
            return f"{faulty_pcs}/{random_qty}"
        else:
            return ""
    
    def safe_extract_value(self, row, column_name):
        """Safely extract value, handling 'Not extracted' and empty values"""
        try:
            # Check if column exists
            if column_name not in row.index:
                return ""
            
            value = str(row[column_name])
            
            # Check if contains "Not extracted"
            if "Not extracted" in value:
                return ""
            
            # Check if NaN or empty string
            if pd.isna(row[column_name]) or value.strip() == "":
                return ""
                
            return value.strip()
        
        except Exception:
            return ""

class UnifiedPDFProcessor:
    """Unified PDF processor"""
    
    def __init__(self):
        self.required_fields = [
            'Claim no', 'Decision', 'Style No', 'Item No', 
            'Delivered quantity', 'Supplier Name', 'Dept.',
            'Order No', 'Random quantity', 'Faulty pcs', 
            'Date of decision', 'Description of faults', 'customer_name'
        ]
        self.pdf_extractor = PDFTextExtractor()
        self.data_mapper = DataMapper()

    def determine_doc_type(self, filename):
        """Determine document type based on filename"""
        filename_upper = filename.upper()
        if filename_upper.startswith('RDR'):
            return "BPH"
        elif filename_upper.startswith('CR'):
            return "OVH"
        else:
            return "UNKNOWN"

    def convert_date_format(self, date_str, source_format="dmy"):
        """Convert date format to MM/DD/YY"""
        if date_str == "Not extracted" or not date_str:
            return date_str
        
        try:
            date_str = date_str.strip()
            
            if source_format == "dmy":
                parts = date_str.split('/')
                if len(parts) == 3:
                    day, month, year = parts
                    if len(year) == 2:
                        return f"{month}/{day}/{year}"
                    else:
                        return f"{month}/{day}/{year[-2:]}"
            elif source_format == "mdy":
                parts = date_str.split('/')
                if len(parts) == 3:
                    month, day, year = parts
                    if len(year) == 2:
                        return date_str
                    else:
                        return f"{month}/{day}/{year[-2:]}"
            
            return date_str
        except Exception as e:
            logger.warning(f"Date format conversion failed: {date_str}, error: {str(e)}")
            return date_str

    def extract_bph_data(self, text, pdf_path):
        """Extract data from BPH PDF"""
        data = {field: "Not extracted" for field in self.required_fields}
        data['customer_name'] = "BPH"

        try:
            # Claim no (original Reclamation ID)
            claim_match = re.search(r'Reclamation\s+ID\s*[\|:]?\s*(\d+)', text, re.IGNORECASE)
            if not claim_match:
                claim_match = re.search(r'Reclamation\s+details\s+report\s+with\s+reclamation\s+ID\s*=\s*(\d+)', text, re.IGNORECASE)
            if claim_match:
                data['Claim no'] = claim_match.group(1)

            # Style No
            style_match = re.search(r'Style\s+No\s*[\|:]?\s*(\d+)', text, re.IGNORECASE)
            if not style_match:
                style_match = re.search(r'Style\s+No\s+Item\s+No[^\d]*(\d+)\s+(\d+)', text, re.IGNORECASE)
                if style_match:
                    data['Style No'] = style_match.group(1)
            elif style_match:
                data['Style No'] = style_match.group(1)

            # Item No
            item_match = re.search(r'Item\s+No\s*[\|:]?\s*(\d+)', text, re.IGNORECASE)
            if not item_match and style_match and len(style_match.groups()) > 1:
                data['Item No'] = style_match.group(2)
            elif item_match:
                data['Item No'] = item_match.group(1)

            # Delivered quantity
            quantity_match = re.search(r'Delivered\s+quantity\s*[\|:]?\s*(\d+)', text, re.IGNORECASE)
            if not quantity_match:
                quantity_match = re.search(r'Delivered\s+quantity\s+Office[^\d]*(\d+)', text, re.IGNORECASE)

            if quantity_match:
                quantity = quantity_match.group(1)
                if len(quantity) == 6:
                    data['Delivered quantity'] = "Not extracted"
                else:
                    data['Delivered quantity'] = quantity

            # Supplier Name
            supplier_patterns = [
                r'OI China\s+(\d{6})\s+([^\n]+?)\s*Dept\./Subdept\.',
                r'OI\s+China\s+(\d{6})\s+([^\n]+?)\s*Dept\./Subdept\.',
                r'OI China\s+(\d{6})\s+([^\n]+?)\s+Dept\./Subdept\.',
                r'OI\s+China\s+(\d{6})\s+([^\n]+?)\s+Dept\./Subdept\.',
            ]

            supplier_name = "Not extracted"
            for pattern in supplier_patterns:
                supplier_match = re.search(pattern, text, re.IGNORECASE)
                if supplier_match:
                    supplier_name = supplier_match.group(2).strip()
                    supplier_name = re.sub(r'\s+', ' ', supplier_name)
                    break

            data['Supplier Name'] = supplier_name

            # Dept.
            dept_match = re.search(r'Dept\./Subdept\.\s*[\|:]?\s*([\d\.]+)', text, re.IGNORECASE)
            if not dept_match:
                dept_match = re.search(r'Dept\./Subdept\.\s+Order\s+No[^\d]*([\d\.]+)\s+(\d+)', text, re.IGNORECASE)
                if dept_match:
                    data['Dept.'] = dept_match.group(1)
            elif dept_match:
                data['Dept.'] = dept_match.group(1)

            # Order No (extracted as string, preserve leading zeros)
            order_match = re.search(r'Order\s+No\s*[\|:]?\s*(\d+)', text, re.IGNORECASE)
            if not order_match and dept_match and len(dept_match.groups()) > 1:
                data['Order No'] = dept_match.group(2)
            elif order_match:
                data['Order No'] = order_match.group(1)

            # Random quantity and Faulty pcs
            sample_faulty_match = re.search(r'Random\s+sample\s*Faulty\s+pieces\s*(\d+)\s*(\d+)', text)
            if sample_faulty_match:
                data['Random quantity'] = sample_faulty_match.group(1)
                data['Faulty pcs'] = sample_faulty_match.group(2)

            # Date of decision
            decision_table_match = re.search(r'Decided by\s+Date of decision\s+Decision', text, re.IGNORECASE)
            if decision_table_match:
                table_start = decision_table_match.end()
                next_line_match = re.search(r'[^\n]+', text[table_start:])
                if next_line_match:
                    data_line = next_line_match.group(0)
                    date_match = re.search(r'(\d+/\d+/\d+)', data_line)
                    if date_match:
                        data['Date of decision'] = self.convert_date_format(date_match.group(1), "mdy")

            if data['Date of decision'] == "Not extracted":
                date_decision_match = re.search(r'Date of decision\s+(\d+/\d+/\d+)', text, re.IGNORECASE)
                if date_decision_match:
                    data['Date of decision'] = self.convert_date_format(date_decision_match.group(1), "mdy")

            if data['Date of decision'] == "Not extracted":
                decided_by_match = re.search(r'Decided by[^\n]*', text, re.IGNORECASE)
                if decided_by_match:
                    end_pos = decided_by_match.end()
                    next_line_match = re.search(r'[^\n]*', text[end_pos:])
                    if next_line_match:
                        next_line = next_line_match.group(0).strip()
                        date_match = re.search(r'(\d+/\d+/\d+)', next_line)
                        if date_match:
                            data['Date of decision'] = self.convert_date_format(date_match.group(1), "mdy")

            # Description of faults
            comment_match = re.search(r'Comment\s+for\s+market[^\n]*([\s\S]*?)(?=Samples|Rework\s+details|Reclamation\s+details\s+report|Printed\s+on|$)', text, re.IGNORECASE)
            if comment_match:
                comment = comment_match.group(1).strip()
                comment = re.sub(r'\n\s*', ' ', comment)
                comment = re.sub(r'\s+', ' ', comment)
                data['Description of faults'] = comment.strip()

            # Decision (original Status)
            if data['Claim no'] != "Not extracted":
                claim_id = data['Claim no']
                pattern3 = (
                    r'Reclamation\s+ID\s*[\s\S]*?' + re.escape(claim_id) +
                    r'\s+([A-Za-z0-9\s\(\)]+?)(?=\s*\n|\s*Style\s+No|\s*Date\s+of\s+delivery)'
                )
                status_match3 = re.search(pattern3, text, re.IGNORECASE | re.DOTALL)

                if status_match3:
                    status_text = status_match3.group(1).strip()
                    status_text = re.sub(r'[\|:\-\*]', '', status_text)
                    status_text = re.sub(r'\s+', ' ', status_text).strip()
                    status_text = re.sub(r'\s+\d+$', '', status_text)

                    if status_text and not re.match(r'^\d+$', status_text):
                        data['Decision'] = status_text

            logger.info(f"Processed BPH document: {pdf_path}")

        except Exception as e:
            logger.error(f"Error processing BPH document {pdf_path}: {str(e)}")

        return data

    def extract_ovh_data(self, text, pdf_path):
        """Extract data from OVH PDF"""
        data = {field: "Not extracted" for field in self.required_fields}
        data['customer_name'] = "OVH"
        
        try:
            # 1. Claim no - 7-digit number before OTTO
            otto_match = re.search(r'(\d{7})\s+OTTO', text)
            if otto_match:
                data['Claim no'] = otto_match.group(1)
            
            # 2. Supplier Name
            incoming_match = re.search(r'Buyin\s+Incoming\s+date\s*[\d/]+\s*([^\n]+?)\s*No\.\s+bowls', text, re.IGNORECASE)
            if incoming_match:
                supplier_text = incoming_match.group(1).strip()
                supplier_text = re.sub(r'\s+', ' ', supplier_text)
                data['Supplier Name'] = supplier_text
            
            # 3. Dept.
            dept_match = re.search(r'dept\.\s*([\d\.]+)', text, re.IGNORECASE)
            if dept_match:
                data['Dept.'] = dept_match.group(1)
            
            # 4. Item No
            cat_match = re.search(r'Cat\.-No\./Page/Block\s*([^\n]*?)(\d{8})', text, re.IGNORECASE)
            if cat_match:
                data['Item No'] = cat_match.group(2)
            
            # 5. Delivered quantity
            style_no_section = re.search(r'Style\s+No\.\s*([^\n]+)', text, re.IGNORECASE)
            if style_no_section:
                style_line = style_no_section.group(1)
                delivered_match = re.search(r'([\d,]+)\s+[A-Z]\s+\d+', style_line)
                if delivered_match:
                    delivered_str = delivered_match.group(1).replace(',', '')
                    data['Delivered quantity'] = delivered_str
            
            # 6. Order No (extracted as string, preserve leading zeros)
            if style_no_section:
                style_line = style_no_section.group(1)
                order_match = re.search(r'[A-Z]\s+(\d{6})', style_line)
                if order_match:
                    data['Order No'] = order_match.group(1)

            # 7. Style No
            style_no_section = re.search(r'Style\s+No\.\s*\n\s*([^\n]+?)\s*\n\s*Inspection result', text, re.IGNORECASE)
            if style_no_section:
                style_line = style_no_section.group(1).strip()
                fields = style_line.split()
                if fields:
                    data['Style No'] = fields[-1]
            else:
                style_no_section = re.search(r'Style\s+No\.\s*([^\n]+)', text, re.IGNORECASE)
                if style_no_section:
                    style_line = style_no_section.group(1).strip()
                    style_match = re.search(r'\d{6}\s+([^\s]+)', style_line)
                    if style_match:
                        data['Style No'] = style_match.group(1)
                    else:
                        fields = style_line.split()
                        if fields:
                            data['Style No'] = fields[-1]

            # 8. Random quantity and Faulty pcs
            pcs_set_match = re.search(r'pcs/\s*set\s*(\d+)\s*(\d+)(?:\s*(\d+))?', text, re.IGNORECASE)
            if pcs_set_match:
                if pcs_set_match.group(3):
                    num1 = int(pcs_set_match.group(1))
                    num2 = int(pcs_set_match.group(2))
                    data['Random quantity'] = str(num1 + num2)
                    data['Faulty pcs'] = pcs_set_match.group(3)
                else:
                    data['Random quantity'] = pcs_set_match.group(1)
                    data['Faulty pcs'] = pcs_set_match.group(2)
            
            # 9. Decision (original Deci.) and Date of decision
            deci_date_pattern = r'([A-Z])\s*/\s*([A-Z])\s*/\s*[^/]+\s*/\s*(\d{1,2}/\d{1,2}/\d{2})'
            deci_date_match = re.search(deci_date_pattern, text)
            
            if deci_date_match:
                data['Decision'] = deci_date_match.group(2)
                data['Date of decision'] = self.convert_date_format(deci_date_match.group(3), "dmy")
            else:
                date_pattern = r'(\d{1,2}/\d{1,2}/\d{2})'
                date_match = re.search(date_pattern, text)
                if date_match:
                    before_date = text[:date_match.start()]
                    deci_pattern = r'([A-Z])\s*/\s*([A-Z])\s*/\s*[^/]+\s*/\s*$'
                    deci_match = re.search(deci_pattern, before_date)
                    if deci_match:
                        data['Decision'] = deci_match.group(2)
                        data['Date of decision'] = self.convert_date_format(date_match.group(1), "dmy")
            
            # 10. Description of faults (no translation, keep original text)
            description_match = re.search(r'Description\s+of\s+faults\s*([\s\S]*?)(?=\s*Rework)', text, re.IGNORECASE)
            if description_match:
                original_description = description_match.group(1).strip()
                cleaned_description = re.sub(r'\n\s*', ' ', original_description)
                cleaned_description = re.sub(r'\s+', ' ', cleaned_description)
                data['Description of faults'] = cleaned_description
            
            logger.info(f"Processed OVH document: {pdf_path}")
            return data
            
        except Exception as e:
            logger.error(f"Error processing OVH document {pdf_path}: {str(e)}")
            return data

    def process_pdfs(self, pdf_files):
        """Process PDF file list (force Order No to string type)"""
        all_data = []

        for pdf_file in pdf_files:
            with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as tmp_file:
                tmp_file.write(pdf_file.getvalue())
                tmp_path = tmp_file.name

            try:
                data = self.extract_data_from_pdf(tmp_path, pdf_file.name)
                data['Source File'] = pdf_file.name
                all_data.append(data)
            finally:
                os.unlink(tmp_path)
        
        # Step 1: Convert to DataFrame and force Order No to string
        df = pd.DataFrame(all_data)
        if 'Order No' in df.columns:
            df['Order No'] = df['Order No'].astype(str)
        
        return df

    def extract_data_from_pdf(self, pdf_path, filename):
        """Extract data from PDF, automatically determine document type"""
        text = self.pdf_extractor.extract_text_from_pdf(pdf_path)

        if text == "Failed to extract text":
            logger.warning(f"Failed to extract text from {pdf_path}")
            data = {field: "Failed to extract text" for field in self.required_fields}
            data['customer_name'] = "Unknown"
            return data

        doc_type = self.determine_doc_type(filename)
        
        if doc_type == "BPH":
            return self.extract_bph_data(text, pdf_path)
        elif doc_type == "OVH":
            return self.extract_ovh_data(text, pdf_path)
        else:
            if "Reclamation" in text and "Reclamation ID" in text:
                return self.extract_bph_data(text, pdf_path)
            elif "OTTO" in text and "Control" in text:
                return self.extract_ovh_data(text, pdf_path)
            else:
                logger.warning(f"Unable to determine document type, using BPH as default: {filename}")
                return self.extract_bph_data(text, pdf_path)

def get_download_link(df, filename, text):
    """Generate download link (ensure Order No and Random check fields are not recognized as dates)"""
    # Copy DataFrame to avoid modifying original data
    df_download = df.copy()
    
    # Process Order No field - add single quote to prevent Excel auto-conversion
    if 'Order No' in df_download.columns:
        df_download['Order No'] = df_download['Order No'].apply(
            lambda x: f"'{x}" if x and x.isdigit() else x
        )
    
    # Process Random check in customer warehouse field - add single quote to prevent Excel auto-conversion to date
    if "Random check in customer warehouse" in df_download.columns:
        df_download["Random check in customer warehouse"] = df_download["Random check in customer warehouse"].apply(
            lambda x: f"'{x}" if x and '/' in x and x.replace('/', '').isdigit() else x
        )
    
    # Export CSV (special fields will have single quotes, Excel will recognize as text)
    csv = df_download.to_csv(index=False, encoding='utf-8-sig')
    b64 = base64.b64encode(csv.encode('utf-8-sig')).decode()
    href = f'<a href="data:file/csv;base64,{b64}" download="{filename}">{text}</a>'
    return href

def style_dataframe_by_customer(df):
    """Add styling to dataframe based on customer type"""
    def color_customer_name(val):
        if val == 'BPH':
            return 'background-color: #e6f3ff; color: #0074D9; font-weight: bold'
        elif val == 'OVH':
            return 'background-color: #e6ffe6; color: #2ECC40; font-weight: bold'
        else:
            return ''
    
    styled_df = df.style.applymap(color_customer_name, subset=['customer_name'])
    return styled_df

def apply_custom_dataframe_styling():
    """Apply custom table styling"""
    st.markdown("""
    <style>
    .dataframe tbody tr th {
        vertical-align: top;
    }
    .dataframe tbody tr td {
        vertical-align: top;
        height: 50px;
    }
    .dataframe thead th {
        text-align: center !important;
    }
    .dataframe td:nth-child(12) {  /* Description of faults column */
        min-width: 500px !important;
        max-width: 500px !important;
        white-space: normal !important;
        word-wrap: break-word !important;
    }
    </style>
    """, unsafe_allow_html=True)

def main():
    """Main function - Streamlit application"""
    st.set_page_config(
        page_title="PDF to Table Converter",
        page_icon="📊",
        layout="wide"
    )

    apply_custom_dataframe_styling()

    st.title("📊 PDF to Table Converter")
    st.markdown("Automatically identify and process BPH and OVH PDF documents, extract data and convert to table format")

    uploaded_files = st.file_uploader(
        "Select PDF files", 
        type="pdf",
        accept_multiple_files=True,
        help="You can upload one or multiple PDF files, system will automatically identify document types"
    )

    if uploaded_files:
        bph_count = 0
        ovh_count = 0
        unknown_count = 0
        
        for file in uploaded_files:
            filename_upper = file.name.upper()
            if filename_upper.startswith('RDR'):
                bph_count += 1
            elif filename_upper.startswith('CR'):
                ovh_count += 1
            else:
                unknown_count += 1
        
        st.success(f"Selected {len(uploaded_files)} files")
        st.info(f"📊 Document type statistics: BPH: {bph_count}, OVH: {ovh_count}, Unknown: {unknown_count}")

        with st.expander("📁 View uploaded file list"):
            for file in uploaded_files:
                filename_upper = file.name.upper()
                if filename_upper.startswith('RDR'):
                    st.write(f"🔵 BPH - {file.name} ({file.size} bytes)")
                elif filename_upper.startswith('CR'):
                    st.write(f"🟢 OVH - {file.name} ({file.size} bytes)")
                else:
                    st.write(f"⚪ Unknown - {file.name} ({file.size} bytes)")

        if st.button("🚀 Start Processing PDF Files", type="primary"):
            processor = UnifiedPDFProcessor()

            with st.spinner("⏳ Processing PDF files, please wait..."):
                # Generate first table (source data)
                df_source = processor.process_pdfs(uploaded_files)
                
                # Reorder columns
                columns_order = ['Source File', 'customer_name'] + [col for col in processor.required_fields if col != 'customer_name']
                existing_columns = [col for col in columns_order if col in df_source.columns]
                df_source = df_source[existing_columns]

                # Generate second table (target format)
                df_target = processor.data_mapper.map_to_target_format(df_source)

                # Display first table
                st.subheader("📋 First Table - Extraction Results (Source Data)")
                bph_processed = len(df_source[df_source['customer_name'] == 'BPH'])
                ovh_processed = len(df_source[df_source['customer_name'] == 'OVH'])
                
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("🔵 BPH Documents", bph_processed)
                with col2:
                    st.metric("🟢 OVH Documents", ovh_processed)
                
                styled_source_df = style_dataframe_by_customer(df_source)
                st.dataframe(
                    styled_source_df, 
                    use_container_width=True,
                    height=min(800, 100 + len(df_source) * 50)
                )

                # Display second table
                st.subheader("📋 Second Table - Mapping Results (Target Format)")
                st.dataframe(
                    df_target, 
                    use_container_width=True,
                    height=min(800, 100 + len(df_target) * 50)
                )

                # Display statistics
                st.subheader("📊 Extraction Statistics")
                total_files = len(df_source)
                stats_data = []

                for customer in ['BPH', 'OVH']:
                    customer_df = df_source[df_source['customer_name'] == customer]
                    if len(customer_df) > 0:
                        for field in processor.required_fields:
                            if field != 'customer_name' and field in df_source.columns:
                                count = len(customer_df[customer_df[field] != "Not extracted"])
                                stats_data.append({
                                    'Customer Type': customer,
                                    'Field Name': field,
                                    'Successfully Extracted': count,
                                    'Total': len(customer_df),
                                    'Success Rate': f"{(count/len(customer_df))*100:.1f}%"
                                })

                stats_df = pd.DataFrame(stats_data)
                if not stats_df.empty:
                    st.dataframe(stats_df, use_container_width=True)

                # Overall statistics
                successful_files = sum(1 for idx, row in df_source.iterrows() if any(
                    row[field] != "Not extracted" and row[field] != "Failed to extract text" 
                    for field in processor.required_fields if field != 'customer_name'
                ))
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Total Files", total_files)
                with col2:
                    st.metric("Successfully Processed Files", successful_files)
                with col3:
                    st.metric("Overall Success Rate", f"{(successful_files/total_files)*100:.1f}%")

                # Provide download
                st.subheader("💾 Download Results")
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                
                col1, col2 = st.columns(2)
                with col1:
                    # First table download
                    source_filename = f"source_data_{timestamp}.csv"
                    st.markdown(get_download_link(df_source, source_filename, "📥 Download First Table (Source Data)"), unsafe_allow_html=True)
                
                with col2:
                    # Second table download
                    target_filename = f"target_data_{timestamp}.csv"
                    st.markdown(get_download_link(df_target, target_filename, "📥 Download Second Table (Target Format)"), unsafe_allow_html=True)

                st.balloons()
                st.success("🎉 All documents processed successfully!")

    else:
        st.info("📝 Please upload PDF files to start processing")

        with st.expander("📖 User Guide"):
            st.markdown("""
            ### Function Description
            - **Supports two types of PDF documents:**
              - 🔵 **BPH**: Reclamation details report
              - 🟢 **OVH**: Control report
            - **Automatically generates two tables:**
              - **First table**: Source data directly extracted from PDF
              - **Second table**: Target format table converted according to mapping rules
            - **Automatically extracts the following field information:**
              - Claim no, Decision, Style No, Item No
              - Delivered quantity, Supplier Name, Dept.
              - Order No, Random quantity, Faulty pcs
              - Date of decision, Description of faults, customer_name

            ### Usage Steps
            1. Upload PDF files (supports mixed upload of BPH and OVH documents)
            2. System automatically identifies document types
            3. Click "Start Processing PDF Files"
            4. View extraction results and statistics for both tables
            5. Download CSV files for both tables separately

            ### Supported File Formats
            - PDF format files only
            - Supports batch upload of multiple files
            - Supports mixed upload of BPH and OVH documents
            """)

if __name__ == "__main__":
    main()

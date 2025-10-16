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

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PDFTextExtractor:
    """使用 pdfplumber 提取 PDF 文本"""
    
    def extract_text_from_pdf(self, pdf_path):
        """使用 pdfplumber 可靠地提取文本"""
        text = ""
        try:
            with pdfplumber.open(pdf_path) as pdf:
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text += page_text + "\n"

            if not text.strip():
                return "无法提取文本"

            return text

        except Exception as e:
            logger.error(f"提取 PDF 文本失败: {str(e)}")
            return "无法提取文本"

class DataMapper:
    """数据映射器 - 将第一张表转换为第二张表"""
    
    def __init__(self):
        self.target_columns = [
            "Picture", "Production Type", "Claim Type", "Vendor", "Claim No.", 
            "Claim Date", "Inspection Date", "Customer", "Dept.", "FID", 
            "TEAM", "QC Trip Leader", "Style NO.", "Order No.", "Article No.", 
            "Relevant shipped Qty", "Quality Digit  （Market)", "Defect Code", 
            "Claim Reason", "QC Responsibility", "Claim Status", "Validate Month", 
            "Claim shipped Qty", "Random check in customer' warehouse", "Re-check in warehouse"
        ]
    
    def map_to_target_format(self, source_df):
        """将源数据映射到目标格式"""
        if source_df.empty:
            return pd.DataFrame(columns=self.target_columns)
        
        mapped_data = []
        
        for index, row in source_df.iterrows():
            mapped_row = self.process_single_row(row)
            mapped_data.append(mapped_row)
        
        result_df = pd.DataFrame(mapped_data, columns=self.target_columns)
        
        # 确保 Random check in customer' warehouse 字段为文本格式
        if "Random check in customer' warehouse" in result_df.columns:
            result_df["Random check in customer' warehouse"] = result_df["Random check in customer' warehouse"].astype(str)
        
        return result_df
    
    def process_single_row(self, source_row):
        """处理单行数据的映射"""
        mapped_row = {col: "" for col in self.target_columns}
        
        # 1. Claim Type逻辑（基于VBA代码逻辑）
        mapped_row["Claim Type"] = self.get_claim_type(source_row)
        
        # 2. Claim Status固定为Failure
        mapped_row["Claim Status"] = "Failure"
        
        # 3. 基本字段映射
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
        
        # 4. 组合字段：Random check in customer' warehouse
        mapped_row["Random check in customer' warehouse"] = self.combine_faulty_random(source_row)
        
        # 5. Validate Month 留空
        mapped_row["Validate Month"] = ""
        
        # 6. 其他字段保持为空（Picture, Production Type, Inspection Date, FID, TEAM, 
        #    QC Trip Leader, Quality Digit, Defect Code, QC Responsibility, Claim shipped Qty, Re-check in warehouse）
        
        return mapped_row
    
    def get_claim_type(self, row):
        """根据Decision列确定Claim Type（基于VBA逻辑）"""
        decision_value = self.safe_extract_value(row, "Decision")
        
        if not decision_value:
            return "Claim"  # 默认值
        
        # 检查是否包含"未提取到"
        if "未提取到" in decision_value:
            return ""
        
        # 根据特定值判断
        if decision_value in ["QD45 (Q)", "Q"]:
            return "Complaint"
        else:
            return "Claim"
    
    def combine_faulty_random(self, row):
        """组合Faulty pcs和Random quantity字段"""
        faulty_pcs = self.safe_extract_value(row, "Faulty pcs")
        random_qty = self.safe_extract_value(row, "Random quantity")
        
        # 只有当两个值都存在且不为空时才组合
        if faulty_pcs and random_qty and faulty_pcs != "未提取到" and random_qty != "未提取到":
            return f"{faulty_pcs}/{random_qty}"
        else:
            return ""
    
    def safe_extract_value(self, row, column_name):
        """安全提取值，处理"未提取到"和空值情况"""
        try:
            # 检查列是否存在
            if column_name not in row.index:
                return ""
            
            value = str(row[column_name])
            
            # 检查是否包含"未提取到"
            if "未提取到" in value:
                return ""
            
            # 检查是否为NaN或空字符串
            if pd.isna(row[column_name]) or value.strip() == "":
                return ""
                
            return value.strip()
        
        except Exception:
            return ""

class UnifiedPDFProcessor:
    """统一的 PDF 处理器"""
    
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
        """根据文件名判断文档类型"""
        filename_upper = filename.upper()
        if filename_upper.startswith('RDR'):
            return "BPH"
        elif filename_upper.startswith('CR'):
            return "OVH"
        else:
            return "UNKNOWN"

    def convert_date_format(self, date_str, source_format="dmy"):
        """转换日期格式为 MM/DD/YY"""
        if date_str == "未提取到" or not date_str:
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
            logger.warning(f"日期格式转换失败: {date_str}, 错误: {str(e)}")
            return date_str

    def extract_bph_data(self, text, pdf_path):
        """从 BPH PDF 提取数据"""
        data = {field: "未提取到" for field in self.required_fields}
        data['customer_name'] = "BPH"

        try:
            # Claim no (原 Reclamation ID)
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
            quantity_match = re.search(r'实际交付数量\s*[\|:]?\s*(\d+)', text, re.IGNORECASE)
            if not quantity_match:
                quantity_match = re.search(r'Delivered\s+quantity\s+Office[^\d]*(\d+)', text, re.IGNORECASE)

            if quantity_match:
                quantity = quantity_match.group(1)
                if len(quantity) == 6:
                    data['Delivered quantity'] = "未提取到"
                else:
                    data['Delivered quantity'] = quantity

            # Supplier Name
            supplier_patterns = [
                r'OI China\s+(\d{6})\s+([^\n]+?)\s*Dept\./Subdept\.',
                r'OI\s+China\s+(\d{6})\s+([^\n]+?)\s*Dept\./Subdept\.',
                r'OI China\s+(\d{6})\s+([^\n]+?)\s+Dept\./Subdept\.',
                r'OI\s+China\s+(\d{6})\s+([^\n]+?)\s+Dept\./Subdept\.',
            ]

            supplier_name = "未提取到"
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

            # Order No（提取时已为字符串，保留前导零）
            order_match = re.search(r'Order\s+No\s*[\|:]?\s*(\d+)', text, re.IGNORECASE)
            if not order_match and dept_match and len(dept_match.groups()) > 1:
                data['Order No'] = dept_match.group(2)
            elif order_match:
                data['Order No'] = order_match.group(1)

            # Random quantity 和 Faulty pcs
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

            if data['Date of decision'] == "未提取到":
                date_decision_match = re.search(r'Date of decision\s+(\d+/\d+/\d+)', text, re.IGNORECASE)
                if date_decision_match:
                    data['Date of decision'] = self.convert_date_format(date_decision_match.group(1), "mdy")

            if data['Date of decision'] == "未提取到":
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

            # Decision (原 Status)
            if data['Claim no'] != "未提取到":
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

            logger.info(f"处理 BPH 文档: {pdf_path}")

        except Exception as e:
            logger.error(f"处理 BPH 文档 {pdf_path} 时出错: {str(e)}")

        return data

    def extract_ovh_data(self, text, pdf_path):
        """从 OVH PDF 提取数据"""
        data = {field: "未提取到" for field in self.required_fields}
        data['customer_name'] = "OVH"
        
        try:
            # 1. Claim no - OTTO前面的一串7位数字
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
            
            # 6. Order No（提取时已为字符串，保留前导零）
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

            # 8. Random quantity 和 Faulty pcs
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
            
            # 9. Decision (原 Deci.) 和 Date of decision
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
            
            # 10. Description of faults（无翻译，保留原始文本）
            description_match = re.search(r'Description\s+of\s+faults\s*([\s\S]*?)(?=\s*Rework)', text, re.IGNORECASE)
            if description_match:
                original_description = description_match.group(1).strip()
                cleaned_description = re.sub(r'\n\s*', ' ', original_description)
                cleaned_description = re.sub(r'\s+', ' ', cleaned_description)
                data['Description of faults'] = cleaned_description
            
            logger.info(f"处理 OVH 文档: {pdf_path}")
            return data
            
        except Exception as e:
            logger.error(f"处理 OVH 文档 {pdf_path} 时出错: {str(e)}")
            return data

    def process_pdfs(self, pdf_files):
        """处理 PDF 文件列表（强制 Order No 为字符串类型）"""
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
        
        # 步骤1：转换为DataFrame并强制Order No为字符串
        df = pd.DataFrame(all_data)
        if 'Order No' in df.columns:
            df['Order No'] = df['Order No'].astype(str)
        
        return df

    def extract_data_from_pdf(self, pdf_path, filename):
        """从 PDF 提取数据，自动判断文档类型"""
        text = self.pdf_extractor.extract_text_from_pdf(pdf_path)

        if text == "无法提取文本":
            logger.warning(f"无法从 {pdf_path} 提取文本")
            data = {field: "无法提取文本" for field in self.required_fields}
            data['customer_name'] = "未知"
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
                logger.warning(f"无法确定文档类型，默认使用 BPH 处理: {filename}")
                return self.extract_bph_data(text, pdf_path)

def get_download_link(df, filename, text):
    """生成下载链接（确保Order No和Random check字段不被识别为日期）"""
    # 复制DataFrame避免修改原数据
    df_download = df.copy()
    
    # 处理Order No字段 - 添加单引号防止Excel自动转换
    if 'Order No' in df_download.columns:
        df_download['Order No'] = df_download['Order No'].apply(
            lambda x: f"'{x}" if x and x.isdigit() else x
        )
    
    # 处理Random check in customer' warehouse字段 - 添加单引号防止Excel自动转换为日期
    if "Random check in customer' warehouse" in df_download.columns:
        df_download["Random check in customer' warehouse"] = df_download["Random check in customer' warehouse"].apply(
            lambda x: f"'{x}" if x and '/' in x and x.replace('/', '').isdigit() else x
        )
    
    # 导出CSV（此时特殊字段带单引号，Excel打开会识别为文本）
    csv = df_download.to_csv(index=False, encoding='utf-8-sig')
    b64 = base64.b64encode(csv.encode('utf-8-sig')).decode()
    href = f'<a href="data:file/csv;base64,{b64}" download="{filename}">{text}</a>'
    return href

def style_dataframe_by_customer(df):
    """根据客户类型为数据框添加样式"""
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
    """应用自定义表格样式"""
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
    .dataframe td:nth-child(12) {  /* Description of faults 列 */
        min-width: 500px !important;
        max-width: 500px !important;
        white-space: normal !important;
        word-wrap: break-word !important;
    }
    </style>
    """, unsafe_allow_html=True)

def main():
    """主函数 - Streamlit 应用"""
    st.set_page_config(
        page_title="PDF 转表格工具",
        page_icon="📊",
        layout="wide"
    )

    apply_custom_dataframe_styling()

    st.title("📊 PDF 转表格工具")
    st.markdown("自动识别并处理 BPH 和 OVH PDF 文档，提取数据并转换为表格格式")

    uploaded_files = st.file_uploader(
        "选择 PDF 文件", 
        type="pdf",
        accept_multiple_files=True,
        help="可以上传一个或多个 PDF 文件，系统会自动识别文档类型"
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
        
        st.success(f"已选择 {len(uploaded_files)} 个文件")
        st.info(f"📊 文档类型统计: BPH: {bph_count} 个, OVH: {ovh_count} 个, 未知: {unknown_count} 个")

        with st.expander("📁 查看上传的文件列表"):
            for file in uploaded_files:
                filename_upper = file.name.upper()
                if filename_upper.startswith('RDR'):
                    st.write(f"🔵 BPH - {file.name} ({file.size} bytes)")
                elif filename_upper.startswith('CR'):
                    st.write(f"🟢 OVH - {file.name} ({file.size} bytes)")
                else:
                    st.write(f"⚪ 未知 - {file.name} ({file.size} bytes)")

        if st.button("🚀 开始处理 PDF 文件", type="primary"):
            processor = UnifiedPDFProcessor()

            with st.spinner("⏳ 正在处理 PDF 文件，请稍候..."):
                # 生成第一张表（源数据）
                df_source = processor.process_pdfs(uploaded_files)
                
                # 重新排列列的顺序
                columns_order = ['Source File', 'customer_name'] + [col for col in processor.required_fields if col != 'customer_name']
                existing_columns = [col for col in columns_order if col in df_source.columns]
                df_source = df_source[existing_columns]

                # 生成第二张表（目标格式）
                df_target = processor.data_mapper.map_to_target_format(df_source)

                # 显示第一张表
                st.subheader("📋 第一张表 - 提取结果（源数据）")
                bph_processed = len(df_source[df_source['customer_name'] == 'BPH'])
                ovh_processed = len(df_source[df_source['customer_name'] == 'OVH'])
                
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("🔵 BPH 文档", bph_processed)
                with col2:
                    st.metric("🟢 OVH 文档", ovh_processed)
                
                styled_source_df = style_dataframe_by_customer(df_source)
                st.dataframe(
                    styled_source_df, 
                    use_container_width=True,
                    height=min(800, 100 + len(df_source) * 50)
                )

                # 显示第二张表
                st.subheader("📋 第二张表 - 映射结果（目标格式）")
                st.dataframe(
                    df_target, 
                    use_container_width=True,
                    height=min(800, 100 + len(df_target) * 50)
                )

                # 显示统计信息
                st.subheader("📊 提取统计")
                total_files = len(df_source)
                stats_data = []

                for customer in ['BPH', 'OVH']:
                    customer_df = df_source[df_source['customer_name'] == customer]
                    if len(customer_df) > 0:
                        for field in processor.required_fields:
                            if field != 'customer_name' and field in df_source.columns:
                                count = len(customer_df[customer_df[field] != "未提取到"])
                                stats_data.append({
                                    '客户类型': customer,
                                    '字段名': field,
                                    '成功提取': count,
                                    '总计': len(customer_df),
                                    '成功率': f"{(count/len(customer_df))*100:.1f}%"
                                })

                stats_df = pd.DataFrame(stats_data)
                if not stats_df.empty:
                    st.dataframe(stats_df, use_container_width=True)

                # 整体统计
                successful_files = sum(1 for idx, row in df_source.iterrows() if any(
                    row[field] != "未提取到" and row[field] != "无法提取文本" 
                    for field in processor.required_fields if field != 'customer_name'
                ))
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("总文件数", total_files)
                with col2:
                    st.metric("成功提取文件数", successful_files)
                with col3:
                    st.metric("整体成功率", f"{(successful_files/total_files)*100:.1f}%")

                # 提供下载
                st.subheader("💾 下载结果")
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                
                col1, col2 = st.columns(2)
                with col1:
                    # 第一张表下载
                    source_filename = f"source_data_{timestamp}.csv"
                    st.markdown(get_download_link(df_source, source_filename, "📥 下载第一张表（源数据）"), unsafe_allow_html=True)
                
                with col2:
                    # 第二张表下载
                    target_filename = f"target_data_{timestamp}.csv"
                    st.markdown(get_download_link(df_target, target_filename, "📥 下载第二张表（目标格式）"), unsafe_allow_html=True)

                st.balloons()
                st.success("🎉 所有文档处理完成！")

    else:
        st.info("📝 请上传 PDF 文件开始处理")

        with st.expander("📖 使用说明"):
            st.markdown("""
            ### 功能说明
            - **支持两种类型的 PDF 文档：**
              - 🔵 **BPH**: Reclamation details report
              - 🟢 **OVH**: Control report
            - **自动生成两张表：**
              - **第一张表**: 从PDF直接提取的源数据
              - **第二张表**: 根据映射规则转换的目标格式表
            - **自动提取以下字段信息：**
              - Claim no, Decision, Style No, Item No
              - Delivered quantity, Supplier Name, Dept.
              - Order No, Random quantity, Faulty pcs
              - Date of decision, Description of faults, customer_name

            ### 使用步骤
            1. 上传 PDF 文件（支持混合上传 BPH 和 OVH 文档）
            2. 系统自动识别文档类型
            3. 点击"开始处理 PDF 文件"
            4. 查看两张表的提取结果和统计信息
            5. 分别下载两张表的CSV文件

            ### 支持的文件格式
            - 仅支持 PDF 格式文件
            - 支持批量上传多个文件
            - 支持混合上传 BPH 和 OVH 文档
            """)

if __name__ == "__main__":
    main()
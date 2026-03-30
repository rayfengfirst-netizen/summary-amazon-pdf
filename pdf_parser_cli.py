"""
Amazon Seller Central PDF批量解析工具 - 命令行版本 (完全动态提取)
支持左右双列布局的 PDF

用法:
    python pdf_parser_cli.py <PDF文件夹路径> <输出Excel文件路径>

示例:
    python pdf_parser_cli.py ./pdf_files ./output.xlsx
"""

import os
import re
import sys
import unicodedata
from datetime import datetime

import pandas as pd


OUTPUT_COLUMNS = [
    '返回目录',
    '平台',
    '店铺号',
    '店铺公司',
    '归属月份',
    '来源数据报告',
    '归属',
    '字段',
    '解释',
    '利润表项目',
    '原币金额',
    '汇率',
    '换算本币（人民币）',
]
ERROR_COLUMNS = [
    '文件名',
    '阶段',
    '原因',
]
HEADER_MAX_Y = 100
BODY_TOP_Y = 100
BODY_BOTTOM_Y = 560
ROW_Y_TOLERANCE = 5
AMOUNT_X_TOLERANCE = 50
MIN_DETAIL_ROW_COUNT = 10
REQUIRED_CATEGORY_KEYS = {'income', 'expenses', 'tax', 'transfers'}
COUNTRY_KEYWORDS = {
    '美国': ['美国', 'us', 'usa', 'united states'],
    '加拿大': ['加拿大', 'canada', 'ca'],
}
CURRENCY_COUNTRY_MAP = {
    'USD': '美国',
    'CAD': '加拿大',
}
CATEGORY_NAME_MAP = {
    'ingresos': 'income',
    'income': 'income',
    'rendimento': 'income',
    'gastos': 'Expenses',
    'expenses': 'Expenses',
    'despesas': 'Expenses',
    'impuesto': 'Tax',
    'tax': 'Tax',
    'impostos': 'Tax',
    'transferencias': 'Transfers',
    'transfers': 'Transfers',
    'transferencias bancarias': 'Transfers',
    'transferencias a cuenta bancaria': 'Transfers',
    'transferencias para conta bancaria': 'Transfers',
    'transferencias para conta bancária': 'Transfers',
}
MONTH_NAME_MAP = {
    'jan': 1,
    'january': 1,
    'ene': 1,
    'enero': 1,
    'fev': 2,
    'feb': 2,
    'february': 2,
    'febrero': 2,
    'mar': 3,
    'march': 3,
    'marzo': 3,
    'abr': 4,
    'apr': 4,
    'april': 4,
    'abril': 4,
    'mai': 5,
    'may': 5,
    'mayo': 5,
    'jun': 6,
    'june': 6,
    'junio': 6,
    'jul': 7,
    'july': 7,
    'julio': 7,
    'ago': 8,
    'aug': 8,
    'august': 8,
    'agosto': 8,
    'set': 9,
    'sep': 9,
    'sept': 9,
    'september': 9,
    'septiembre': 9,
    'out': 10,
    'oct': 10,
    'october': 10,
    'octubre': 10,
    'nov': 11,
    'november': 11,
    'noviembre': 11,
    'dez': 12,
    'dec': 12,
    'december': 12,
    'diciembre': 12,
}
DEFAULT_COUNTRY_CONFIGS = {
    '美国': {
        'platform': 'AMZ',
        'store_code': 'YR',
        'company': '苏州乐竹壹冉网络科技有限公司',
        'report_type': 'summary账单',
        'exchange_rate': 7.023,
    },
    '加拿大': {
        'platform': 'AMZ',
        'store_code': 'YR',
        'company': '苏州乐竹壹冉网络科技有限公司',
        'report_type': 'summary账单',
        'exchange_rate': 5.0945,
    },
}
EMBEDDED_TEMPLATE_ROWS = [
    ('美国', 'income', 'FBA product sales', 'FBA商品销售', '主营业务收入'),
    ('美国', 'income', 'FBA product sale refunds', 'FBA销售退款额', '主营业务收入'),
    ('美国', 'income', 'FBA inventory credit', 'FBA库存赔偿', '其他业务收入'),
    ('美国', 'income', 'FBA liquidation proceeds', '清算收入', '其他业务收入'),
    ('美国', 'income', 'Shipping credits', '买家运费', '主营业务收入'),
    ('美国', 'income', 'Shipping credit refunds', '买家运费退款额', '主营业务收入'),
    ('美国', 'income', 'Promotional rebates', '促销折扣', '主营业务收入'),
    ('美国', 'income', 'Promotional rebate refunds', '促销折扣退款额', '主营业务收入'),
    ('美国', 'Expenses', 'FBA selling fees', 'FBA销售费用销售佣金', '销售费用'),
    ('美国', 'Expenses', 'Selling fee refunds', 'FBA销售费用销售佣金退款', '销售费用'),
    ('美国', 'Expenses', 'FBA transaction fees', 'FBA物流配送费', '销售费用'),
    ('美国', 'Expenses', 'FBA transaction fee refunds', 'FBA物流配送费退款', '销售费用'),
    ('美国', 'Expenses', 'FBA inventory and inbound services fees', 'FBA仓储费-FBA库存与入库服务费', '销售费用'),
    ('美国', 'Expenses', 'Service fees', '服务费每月月租费订阅费', '销售费用'),
    ('美国', 'Expenses', 'Refund administration fees', '退款管理费，管理服务费', '销售费用'),
    ('美国', 'Expenses', 'Adjustments', '调整费用', '销售费用'),
    ('美国', 'Expenses', 'Cost of Advertising', '广告费', '销售费用'),
    ('美国', 'Expenses', 'Liquidations fees', '平台其他费-清算费', '销售费用'),
    ('美国', 'Expenses', 'Receivables Deductions', '应收账款扣减', '销售费用'),
    ('美国', 'Tax', 'Product, shipping, gift wrap taxes and regulatory fee collected', '销售税-卖家运费税', '应交税费'),
    ('美国', 'Tax', 'Product, shipping, gift wrap taxes and regulatory fee refunde', '销售税退款-VAT/GST', '应交税费'),
    ('美国', 'Tax', 'Amazon Obligated Tax and Regulatory Fee Withheld', '亚马逊已代扣的应交税款和监督费用', '应交税费-已交税费'),
    ('美国', 'Transfers', 'Transfers to bank account', '转账成功汇总', '应收账款'),
    ('加拿大', 'income', 'FBA product sales', 'FBA商品销售', '主营业务收入'),
    ('加拿大', 'income', 'FBA product sale refunds', 'FBA销售退款额', '主营业务收入'),
    ('加拿大', 'income', 'FBA inventory credit', 'FBA库存赔偿', '其他业务收入'),
    ('加拿大', 'income', 'Shipping credits', '买家运费', '主营业务收入'),
    ('加拿大', 'income', 'Shipping credit refunds', '买家运费退款额', '主营业务收入'),
    ('加拿大', 'income', 'Promotional rebates', '促销折扣', '主营业务收入'),
    ('加拿大', 'Expenses', 'FBA selling fees', 'FBA销售费用销售佣金', '销售费用'),
    ('加拿大', 'Expenses', 'Selling fee refunds', 'FBA销售费用销售佣金退款', '销售费用'),
    ('加拿大', 'Expenses', 'FBA transaction fees', 'FBA物流配送费', '销售费用'),
    ('加拿大', 'Expenses', 'FBA transaction fee refunds', 'FBA物流配送费退款', '销售费用'),
    ('加拿大', 'Expenses', 'Other transaction fees', '其他交易费用', '销售费用'),
    ('加拿大', 'Expenses', 'Other transaction fee refunds', '其他交易费用退款', '销售费用'),
    ('加拿大', 'Expenses', 'FBA inventory and inbound services fees', 'FBA仓储费-FBA库存与入库服务费', '销售费用'),
    ('加拿大', 'Expenses', 'Service fees', '服务费每月月租费订阅费', '销售费用'),
    ('加拿大', 'Expenses', 'Refund administration fees', '退款管理费，管理服务费', '销售费用'),
    ('加拿大', 'Expenses', 'Adjustments', '调整费用', '销售费用'),
    ('加拿大', 'Expenses', 'Cost of Advertising', '广告费', '销售费用'),
    ('加拿大', 'Tax', 'Product, shipping, gift wrap taxes and regulatory fee collected', '销售税-卖家运费税', '应交税费'),
    ('加拿大', 'Tax', 'Amazon Obligated Tax and Regulatory Fee Withheld', '亚马逊已代扣的应交税款和监督费用', '应交税费-已交税费'),
    ('加拿大', 'Transfers', 'Transfers to bank account', '转账成功汇总', '应收账款'),
    ('', 'income', 'Seller fulfilled product sales', '自配送商品销售', '主营业务收入'),
    ('', 'income', 'Seller fulfilled product sale refunds', '自配送商品销售退款', '主营业务收入'),
    ('', 'income', 'Product sales (non-FBA)', '非FBA商品销售', '主营业务收入'),
    ('', 'income', 'Product sale refunds (non-FBA)', '非FBA商品销售退款', '主营业务收入'),
    ('', 'income', 'FBA Liquidations proceeds adjustments', '清算收入调整', '其他业务收入'),
    ('', 'income', 'Gift wrap credits', '礼品包装费收入', '主营业务收入'),
    ('', 'income', 'Gift wrap credit refunds', '礼品包装费退款', '主营业务收入'),
    ('', 'income', 'A-to-z Guarantee claims', 'A-to-z索赔补偿', '其他业务收入'),
    ('', 'income', 'Chargebacks', '拒付调整', '其他业务收入'),
    ('', 'income', 'Amazon Shipping Reimbursement Adjustments', '亚马逊运费补偿调整', '主营业务收入'),
    ('', 'income', 'SAFE-T reimbursement', 'SAFE-T赔付款', '其他业务收入'),
    ('', 'Expenses', 'Seller fulfilled selling fees', '自配送销售费用销售佣金', '销售费用'),
    ('', 'Expenses', 'Shipping label purchases', '物流面单购买费', '销售费用'),
    ('', 'Expenses', 'Shipping label refunds', '物流面单退款', '销售费用'),
    ('', 'Expenses', 'Carrier shipping label adjustments', '承运人面单调整', '销售费用'),
    ('', 'Expenses', 'Refund for Advertiser', '广告退款', '销售费用'),
    ('', 'Expenses', 'Amazon Shipping Charge Adjustments', '亚马逊运费收费调整', '销售费用'),
    ('', 'Tax', 'Product, shipping and gift wrap taxes collected', '产品运费及礼品包装税已代收', '应交税费'),
    ('', 'Tax', 'Product, shipping and gift wrap taxes refunded', '产品运费及礼品包装税退款', '应交税费'),
    ('', 'Tax', 'Product, shipping, gift wrap taxes and regulatory fee refunded', '产品运费礼品包装税及监管费退款', '应交税费'),
    ('', 'Tax', 'Amazon obligated tax withheld', '亚马逊已代扣税费', '应交税费-已交税费'),
    ('', 'Transfers', 'Failed transfers to bank account', '转账至银行账户失败', '应收账款'),
    ('', 'Transfers', 'Charges to credit card and other debt recovery', '信用卡扣款及其他债务追偿', '应收账款'),
    ('', 'Transfers', 'Disburse to Amazon Gift Card balance', '转入亚马逊礼品卡余额', '应收账款'),
    ('', 'Transfers', 'Account Types Included - Standard Orders, Invoiced Orders', '账户类型含标准订单及发票订单', '应收账款'),
    ('', 'income', '=========================================----', '版式分隔占位行', '其他业务收入'),
]


def strip_accents(text):
    if text is None:
        return ''
    normalized = unicodedata.normalize('NFKD', str(text))
    return ''.join(char for char in normalized if not unicodedata.combining(char))


def normalize_key(text):
    text = strip_accents(text).lower().strip()
    text = re.sub(r'\s+', ' ', text)
    return text


def normalize_category_name(category):
    normalized = normalize_key(category)
    return CATEGORY_NAME_MAP.get(normalized, category)


def normalize_field_name(field_name):
    return re.sub(r'\s+', ' ', str(field_name or '').strip())


def is_noise_field(text):
    normalized = normalize_field_name(text)
    if not normalized:
        return True

    field_key = normalize_key(normalized)
    if field_key in {'totals', 'subtotals', 'subtotales', 'subtotais'}:
        return True

    # Ignore layout separators extracted from some Unified Summary PDFs.
    if re.fullmatch(r'[\W_=\-.\s]+', normalized):
        return True

    separator_only = re.sub(r'[\s\-]', '', normalized)
    if separator_only and set(separator_only) == {'='}:
        return True

    return False


def load_amz_template_config():
    field_mappings = {}
    fallback_mappings = {}
    for country, category, field_name, explanation, profit_item in EMBEDDED_TEMPLATE_ROWS:
        category = normalize_category_name(category)
        field_name = normalize_field_name(field_name)
        mapping = {
            'explanation': explanation,
            'profit_item': profit_item,
        }
        if country and category and field_name:
            field_mappings[(country, normalize_key(category), normalize_key(field_name))] = mapping
        if category and field_name:
            fallback_mappings[(normalize_key(category), normalize_key(field_name))] = mapping

    return {
        'template_file': 'built_in_amz_mapping',
        'columns': OUTPUT_COLUMNS,
        'field_mappings': field_mappings,
        'fallback_mappings': fallback_mappings,
        'country_defaults': DEFAULT_COUNTRY_CONFIGS,
    }


def get_mapping_for_item(template_config, country, category, field_name):
    category_key = normalize_key(category)
    field_key = normalize_key(field_name)
    mapping = template_config['field_mappings'].get((country, category_key, field_key))
    if mapping:
        return mapping
    return template_config['fallback_mappings'].get((category_key, field_key))


def detect_country(header_info, source_file, template_config):
    source_text = normalize_key(source_file)
    tokens = set(re.findall(r'[a-z]+', source_text))
    for country, keywords in COUNTRY_KEYWORDS.items():
        for keyword in keywords:
            normalized_keyword = normalize_key(keyword)
            if ' ' in normalized_keyword:
                if normalized_keyword in source_text:
                    return country
            elif normalized_keyword in tokens:
                return country
            elif normalized_keyword in source_text and len(normalized_keyword) > 3:
                return country

    for country, keywords in COUNTRY_KEYWORDS.items():
        if any(keyword in source_text for keyword in keywords if len(normalize_key(keyword)) > 3):
            return country

    currency = str(header_info.get('currency') or '').upper()
    if currency in CURRENCY_COUNTRY_MAP:
        return CURRENCY_COUNTRY_MAP[currency]

    if len(template_config['country_defaults']) == 1:
        return next(iter(template_config['country_defaults']))
    return ''


def parse_month_start(text):
    if not text:
        return None

    text = str(text).strip()
    normalized = normalize_key(text)
    month_pattern = '|'.join(sorted(MONTH_NAME_MAP.keys(), key=len, reverse=True))

    match = re.search(r'(\d{4})[./-](\d{1,2})[./-]\d{1,2}', text)
    if match:
        year, month = int(match.group(1)), int(match.group(2))
        return datetime(year, month, 1)

    match = re.search(r'(\d{1,2})[./-](\d{1,2})[./-](\d{4})', text)
    if match:
        first, second, year = int(match.group(1)), int(match.group(2)), int(match.group(3))
        month = first if first <= 12 else second
        return datetime(year, month, 1)

    match = re.search(rf'(\d{{4}})\s*({month_pattern})', normalized)
    if match:
        return datetime(int(match.group(1)), MONTH_NAME_MAP[match.group(2)], 1)

    match = re.search(rf'({month_pattern})\s*(\d{{4}})', normalized)
    if match:
        return datetime(int(match.group(2)), MONTH_NAME_MAP[match.group(1)], 1)

    match = re.search(rf'({month_pattern})\s+\d{{1,2}},?\s+(\d{{4}})', normalized)
    if match:
        return datetime(int(match.group(2)), MONTH_NAME_MAP[match.group(1)], 1)

    match = re.search(rf'(\d{{1,2}})\s+({month_pattern})\s+(\d{{4}})', normalized)
    if match:
        return datetime(int(match.group(3)), MONTH_NAME_MAP[match.group(2)], 1)

    match = re.search(rf'({month_pattern})\s+(\d{{4}})', normalized)
    if match:
        return datetime(int(match.group(2)), MONTH_NAME_MAP[match.group(1)], 1)

    return None


def get_belong_month(header_info, source_file):
    for value in (header_info.get('time'), source_file):
        month_start = parse_month_start(value)
        if month_start:
            return month_start
    return ''


def validate_header_info(header_info, source_file):
    required_fields = {
        'display_name': '店铺展示名',
        'legal_name': '店铺公司名',
        'time': '账期',
        'currency': '币种',
    }
    missing = [label for key, label in required_fields.items() if not header_info.get(key)]
    if missing:
        raise ValueError(
            f'{source_file}: 头部信息缺失（{", ".join(missing)}），当前脚本仅支持固定版式的 Amazon summary PDF。'
        )


def validate_financial_layout(financial_data, source_file, page_width, page_height, spans):
    if page_height <= BODY_BOTTOM_Y:
        raise ValueError(
            f'{source_file}: 页面高度为 {page_height:.1f}，不满足当前脚本支持的正文区域（Y≈{BODY_TOP_Y}-{BODY_BOTTOM_Y}）。'
        )

    mid_x = page_width / 2
    body_spans = [span for span in spans if BODY_TOP_Y < span['y'] < BODY_BOTTOM_Y]
    left_body_spans = [span for span in body_spans if span['x'] < mid_x]
    right_body_spans = [span for span in body_spans if span['x'] >= mid_x]
    if not left_body_spans or not right_body_spans:
        raise ValueError(f'{source_file}: 未识别到左右双列正文区域，当前脚本仅支持双列 Amazon summary PDF。')

    detail_rows = [item for item in financial_data if item['level'] == 2]
    if len(detail_rows) < MIN_DETAIL_ROW_COUNT:
        raise ValueError(
            f'{source_file}: 仅提取到 {len(detail_rows)} 条明细，低于最小阈值 {MIN_DETAIL_ROW_COUNT}，疑似版式不匹配。'
        )

    category_keys = {
        normalize_key(normalize_category_name(item['category']))
        for item in financial_data
        if item['level'] == 1
    }
    if not REQUIRED_CATEGORY_KEYS.issubset(category_keys):
        raise ValueError(
            f'{source_file}: 未完整识别四大分类 {sorted(REQUIRED_CATEGORY_KEYS)}，当前脚本仅支持固定版式的 summary PDF。'
        )


def get_original_amount(item):
    values = []
    for key in ('debits', 'credits'):
        value = item.get(key)
        if value in ('', None):
            continue
        values.append(float(value))
    if not values:
        return 0
    if len(values) == 1:
        return values[0]
    return sum(values)


def extract_all_text_with_positions(page):
    """从PDF页面提取所有文本及其位置"""
    blocks = page.get_text('dict')['blocks']
    
    spans = []
    for block in blocks:
        if 'lines' in block:
            for line in block['lines']:
                for span in line['spans']:
                    text = span['text'].strip()
                    if text:
                        spans.append({
                            'x': round(span['bbox'][0], 1),
                            'y': round(span['bbox'][1], 1),
                            'x2': round(span['bbox'][2], 1),
                            'text': text
                        })
    
    return spans


def parse_number(text):
    """尝试将文本解析为数字"""
    if not text:
        return None
    
    text = text.strip()
    if text == '0':
        return 0.0
    
    # 移除货币符号和空格
    text = re.sub(r'[$€£¥]', '', text)
    text = text.replace(' ', '')
    
    # 检测是否为负数
    is_negative = text.startswith('-') or (text.startswith('(') and text.endswith(')'))
    text = text.replace('-', '').replace('(', '').replace(')', '')
    
    # 尝试解析
    try:
        clean = text.replace(',', '')
        result = float(clean)
        return -result if is_negative else result
    except:
        try:
            clean = text.replace('.', '').replace(',', '.')
            result = float(clean)
            return -result if is_negative else result
        except:
            return None


def extract_header_info(spans, page_width):
    """从文本数据中提取头部信息"""
    header_info = {
        'display_name': '',
        'legal_name': '',
        'time': '',
        'currency': ''
    }
    
    for span in spans:
        text = span['text']
        y = span['y']
        x = span['x']
        
        # Display name - 支持西班牙语、英语、葡萄牙语标签
        if 'Nombre para mostrar' in text or 'Display name' in text or 'Nome de exibição' in text:
            # 在所有 spans 中找同一行 Y 坐标附近且 X 更大的元素
            candidates = [s for s in spans if abs(s['y'] - y) < ROW_Y_TOLERANCE and s['x'] > x 
                         and 'Nombre' not in s['text'] and 'Display' not in s['text'] 
                         and 'Legal' not in s['text'] and 'Nome' not in s['text']]
            if candidates:
                # 选择 X 最小的那个（最接近标签的）
                candidates.sort(key=lambda s: s['x'])
                header_info['display_name'] = candidates[0]['text']
        
        # Legal name - 支持西班牙语、英语、葡萄牙语标签
        if 'Nombre legal' in text or 'Legal name' in text or 'Nome legal' in text or 'Nome jurídico' in text:
            candidates = [s for s in spans if abs(s['y'] - y) < ROW_Y_TOLERANCE and s['x'] > x 
                         and 'Nombre' not in s['text'] and 'Display' not in s['text'] 
                         and 'Legal' not in s['text'] and 'Nome' not in s['text'] and 'jurídico' not in s['text']]
            if candidates:
                candidates.sort(key=lambda s: s['x'])
                header_info['legal_name'] = candidates[0]['text']
        
        # Time - 只处理页面顶部区域
        if y < HEADER_MAX_Y:
            # 支持西班牙语、英语、葡萄牙语
            if 'Actividad de la cuenta' in text or 'Account activity' in text or 'Atividade de conta' in text:
                header_info['time'] = text
        
            # Currency - 支持西班牙语、英语、葡萄牙语
            if 'importes en' in text.lower():
                match = re.search(r'importes en (\w+)', text, re.IGNORECASE)
                if match:
                    header_info['currency'] = match.group(1)
            elif 'amounts in' in text.lower():
                match = re.search(r'amounts in (\w+)', text, re.IGNORECASE)
                if match:
                    header_info['currency'] = match.group(1)
            elif 'valores em' in text.lower():
                match = re.search(r'valores em (\w+)', text, re.IGNORECASE)
                if match:
                    header_info['currency'] = match.group(1)
            elif 'quantias em' in text.lower():
                match = re.search(r'quantias em (\w+)', text, re.IGNORECASE)
                if match:
                    header_info['currency'] = match.group(1)
    
    return header_info


def extract_financial_data_dual_column(spans, page_width):
    """
    从双列布局的 PDF 中提取财务数据
    支持西班牙语、英语、葡萄牙语
    """
    # 类别关键词 (西班牙语、英语、葡萄牙语)
    category_keywords = [
        'Ingresos', 'Income', 'Rendimento',        # 收入
        'Gastos', 'Expenses', 'Despesas',           # 支出
        'Impuesto', 'Tax', 'Impostos',              # 税
        'Transferencias', 'Transfers', 'Transferências'  # 转账
    ]
    
    # 需要排除的文本 (西班牙语、英语、葡萄牙语)
    exclude_texts = [
        '0', '', 
        'Débitos', 'Créditos', 'Debits', 'Credits', 'Débito', 'Crédito',
        'Totales', 'Totals', 'Totais',
        'Resúmenes', 'Summaries', 'Resumos',
        'Página', 'Page', 'Página',
        'Subtotales', 'Subtotals', 'Subtotais'
    ]
    
    # 分割点 (页面中间)
    mid_x = page_width / 2
    
    # 将 spans 分成左右两列
    left_spans = [s for s in spans if s['x'] < mid_x and BODY_TOP_Y < s['y'] < BODY_BOTTOM_Y]
    right_spans = [s for s in spans if s['x'] >= mid_x and BODY_TOP_Y < s['y'] < BODY_BOTTOM_Y]
    
    # 按 Y 坐标排序
    left_spans.sort(key=lambda s: (s['y'], s['x']))
    right_spans.sort(key=lambda s: (s['y'], s['x']))
    
    def process_column(column_spans):
        """处理单列数据"""
        data = []
        current_category = None
        
        # 首先找到 Débitos 和 Créditos 的位置
        debits_x = None
        credits_x = None
        for span in column_spans:
            if 'Débitos' in span['text'] or 'Debits' in span['text']:
                debits_x = span['x']
            if 'Créditos' in span['text'] or 'Credits' in span['text']:
                credits_x = span['x']
        if debits_x is None or credits_x is None:
            raise ValueError('未找到 Debits/Credits 列头，当前脚本仅支持固定双列表格版式。')
        
        # 按 Y 坐标分组成行
        rows = []
        current_y = None
        current_row = []
        
        for span in column_spans:
            if current_y is None or abs(span['y'] - current_y) > ROW_Y_TOLERANCE:
                if current_row:
                    rows.append({'y': current_y, 'items': current_row})
                current_y = span['y']
                current_row = [span]
            else:
                current_row.append(span)
        
        if current_row:
            rows.append({'y': current_y, 'items': current_row})
        
        # 处理每一行
        for row in rows:
            items = sorted(row['items'], key=lambda x: x['x'])
            texts = [item['text'] for item in items]
            
            # 检查是否是类别标题行
            found_category = None
            for keyword in category_keywords:
                for text in texts:
                    if keyword == text or (keyword in text and len(text) < len(keyword) + 5):
                        found_category = keyword
                        break
                if found_category:
                    break
            
            if found_category:
                # 找到对应的数值
                debits = ''
                credits = ''
                
                for item in items:
                    num = parse_number(item['text'])
                    if num is not None and item['text'] != found_category:
                        if debits_x and abs(item['x'] - debits_x) < AMOUNT_X_TOLERANCE:
                            debits = num
                        elif credits_x and abs(item['x'] - credits_x) < AMOUNT_X_TOLERANCE:
                            credits = num
                
                current_category = found_category
                data.append({
                    'level': 1,
                    'category': found_category,
                    'item': found_category,
                    'debits': debits,
                    'credits': credits
                })
            elif current_category:
                # 数据行
                item_name = None
                debits = ''
                credits = ''
                
                for item in items:
                    text = item['text']
                    num = parse_number(text)
                    
                    if num is not None:
                        if debits_x and abs(item['x'] - debits_x) < AMOUNT_X_TOLERANCE:
                            debits = num
                        elif credits_x and abs(item['x'] - credits_x) < AMOUNT_X_TOLERANCE:
                            credits = num
                    else:
                        # 项目名称
                        if text not in exclude_texts and len(text) > 3 and item_name is None:
                            if not any(kw in text for kw in ['Débitos', 'Créditos', 'Totales']):
                                if not is_noise_field(text):
                                    item_name = text
                
                if item_name:
                    data.append({
                        'level': 2,
                        'category': current_category,
                        'item': item_name,
                        'debits': debits if debits != '' else 0,
                        'credits': credits if credits != '' else 0
                    })
        
        return data
    
    # 处理左右两列
    left_data = process_column(left_spans)
    right_data = process_column(right_spans)
    
    # 合并数据
    all_data = left_data + right_data
    
    return all_data


def parse_pdf(pdf_path):
    """解析单个PDF文件"""
    try:
        import fitz
    except ImportError as exc:
        raise ImportError('缺少 PyMuPDF 依赖，请先安装 fitz/PyMuPDF') from exc

    source_file = os.path.basename(pdf_path)
    doc = fitz.open(pdf_path)
    try:
        if doc.page_count != 1:
            raise ValueError(f'{source_file}: 仅支持单页 PDF，当前文件共有 {doc.page_count} 页。')

        page = doc[0]
        page_width = page.rect.width
        page_height = page.rect.height

        # 提取所有文本
        spans = extract_all_text_with_positions(page)

        # 提取头部信息
        header_info = extract_header_info(spans, page_width)
        header_info['source_file'] = source_file
        validate_header_info(header_info, source_file)

        # 提取财务数据 (双列布局)
        financial_data = extract_financial_data_dual_column(spans, page_width)
        validate_financial_layout(financial_data, source_file, page_width, page_height, spans)
        return header_info, financial_data
    finally:
        doc.close()


def process_pdf_to_rows(pdf_path, template_config):
    """处理单个PDF文件，返回 AMZ 模板结构的数据行列表"""
    header_info, financial_data = parse_pdf(pdf_path)
    source_file = header_info['source_file']
    country = detect_country(header_info, header_info['source_file'], template_config)
    if not country:
        raise ValueError(f'{source_file}: 无法根据文件名或币种识别国家，当前脚本仅支持美国/加拿大站点。')

    defaults = template_config['country_defaults'].get(country, {})
    if not defaults:
        raise ValueError(f'{source_file}: 未找到国家 {country} 的默认配置。')

    exchange_rate = defaults.get('exchange_rate', 0) or 0
    belong_month = get_belong_month(header_info, header_info['source_file'])
    if not belong_month:
        raise ValueError(f'{source_file}: 无法识别归属月份。')

    rows = []
    unmapped_items = []
    ignored_zero_unmapped_items = []

    for item in financial_data:
        if item['level'] != 2:
            continue

        field_name = normalize_field_name(item['item'])
        if not field_name or is_noise_field(field_name):
            continue

        field_key = normalize_key(field_name)
        if 'subtotal' in field_key or 'subtotai' in field_key or field_key == 'totals':
            continue

        category = normalize_category_name(item['category'])
        amount = get_original_amount(item)
        mapping = get_mapping_for_item(template_config, country, category, field_name)

        if not mapping:
            if amount == 0:
                ignored_zero_unmapped_items.append((category, field_name))
                continue
            unmapped_items.append((category, field_name))
            continue

        converted_amount = round(amount * exchange_rate, 6) if exchange_rate else 0

        row = {
            '返回目录': country,
            '平台': defaults.get('platform', 'AMZ'),
            '店铺号': header_info['display_name'] or defaults.get('store_code', ''),
            '店铺公司': header_info['legal_name'] or defaults.get('company', ''),
            '归属月份': belong_month,
            '来源数据报告': defaults.get('report_type', 'summary账单'),
            '归属': category,
            '字段': field_name,
            '解释': mapping['explanation'],
            '利润表项目': mapping['profit_item'],
            '原币金额': amount,
            '汇率': exchange_rate,
            '换算本币（人民币）': converted_amount,
        }
        rows.append(row)

    if unmapped_items:
        missing = '; '.join(f'{category} / {field_name}' for category, field_name in sorted(set(unmapped_items)))
        raise ValueError(f'{source_file}: 检测到未映射字段，已按强校验中止：{missing}')

    return rows, {
        'country': country,
        'currency': header_info['currency'],
        'display_name': header_info['display_name'],
        'legal_name': header_info['legal_name'],
        'time': header_info['time'],
        'unmapped_items': unmapped_items,
        'ignored_zero_unmapped_items': ignored_zero_unmapped_items,
    }


def process_pdf_folder(input_folder, output_file, progress_callback=None):
    """批量处理文件夹中的 PDF，并输出 Excel。"""
    if not os.path.isdir(input_folder):
        raise ValueError(f"输入文件夹不存在: {input_folder}")

    if not output_file.endswith('.xlsx'):
        output_file += '.xlsx'

    template_config = load_amz_template_config()
    pdf_files = sorted(f for f in os.listdir(input_folder) if f.lower().endswith('.pdf'))

    if not pdf_files:
        raise ValueError("文件夹中没有PDF文件")

    all_rows = []
    error_rows = []
    success_count = 0

    if progress_callback:
        progress_callback({
            'stage': 'starting',
            'total_files': len(pdf_files),
            'processed_files': 0,
            'success_count': 0,
            'failure_count': 0,
            'current_file': '',
            'errors': [],
        })

    for i, pdf_file in enumerate(pdf_files, 1):
        pdf_path = os.path.join(input_folder, pdf_file)
        try:
            rows, metadata = process_pdf_to_rows(pdf_path, template_config)
            all_rows.extend(rows)
            success_count += 1
            status = {
                'index': i,
                'file': pdf_file,
                'status': 'success',
                'country': metadata['country'],
                'display_name': metadata['display_name'],
                'legal_name': metadata['legal_name'],
                'currency': metadata['currency'],
                'time': metadata['time'],
                'row_count': len(rows),
            }
        except Exception as e:
            error_rows.append({
                '文件名': pdf_file,
                '阶段': 'process_pdf_to_rows',
                '原因': str(e),
            })
            status = {
                'index': i,
                'file': pdf_file,
                'status': 'error',
                'reason': str(e),
            }

        if progress_callback:
            progress_callback({
                'stage': 'processing',
                'total_files': len(pdf_files),
                'processed_files': i,
                'success_count': success_count,
                'failure_count': len(error_rows),
                'current_file': pdf_file,
                'last_result': status,
                'errors': error_rows[-20:],
            })

    if all_rows or error_rows:
        df = pd.DataFrame(all_rows).reindex(columns=OUTPUT_COLUMNS)
        error_df = pd.DataFrame(error_rows).reindex(columns=ERROR_COLUMNS)
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='AMZ')
            if not error_df.empty:
                error_df.to_excel(writer, index=False, sheet_name='Errors')
    else:
        raise ValueError("没有成功解析任何PDF文件")

    result = {
        'total_files': len(pdf_files),
        'success_count': success_count,
        'failure_count': len(error_rows),
        'total_rows': len(all_rows),
        'output_file': output_file,
        'errors': error_rows,
    }

    if progress_callback:
        progress_callback({
            'stage': 'completed',
            **result,
        })

    return result


def main():
    # 处理命令行参数
    if len(sys.argv) < 3:
        print("用法: python pdf_parser_cli.py <PDF文件夹路径> <输出Excel文件路径>")
        print("示例: python pdf_parser_cli.py ./pdf_files ./output.xlsx")
        sys.exit(1)
    
    input_folder = sys.argv[1]
    output_file = sys.argv[2]
    
    # 验证输入文件夹
    if not os.path.isdir(input_folder):
        print(f"错误: 输入文件夹不存在: {input_folder}")
        sys.exit(1)
    
    try:
        print("开始处理...")

        def cli_progress(payload):
            stage = payload.get('stage')
            if stage == 'starting':
                print(f"发现 {payload['total_files']} 个PDF文件")
                print("使用模板: built_in_amz_mapping")
                print("-" * 50)
            elif stage == 'processing':
                last = payload.get('last_result', {})
                index = last.get('index', payload.get('processed_files', 0))
                total = payload.get('total_files', 0)
                if last.get('status') == 'success':
                    print(f"[{index}/{total}] [OK] {last['file']}")
                    print(f"    国家: {last.get('country') or '未识别'}")
                    print(f"    店铺: {last.get('display_name', '')}")
                    print(f"    公司: {last.get('legal_name', '')}")
                    print(f"    账期: {last.get('time', '')}")
                    print(f"    币种: {last.get('currency', '')}")
                    print(f"    AMZ行数: {last.get('row_count', 0)}")
                elif last.get('status') == 'error':
                    print(f"[{index}/{total}] [ERROR] {last['file']}")
                    print(f"    原因: {last.get('reason', '')}")
            elif stage == 'completed':
                print("-" * 50)
                print("正在生成Excel文件...")
                print()
                print("=" * 50)
                print("处理完成!")
                print(f"  成功: {payload['success_count']} 个文件")
                print(f"  总行数: {payload['total_rows']}")
                print(f"  失败: {payload['failure_count']} 个文件")
                print(f"  输出文件: {payload['output_file']}")
                if payload['errors']:
                    print("  失败详情: 见 Excel 的 Errors sheet")
                print("=" * 50)

        process_pdf_folder(input_folder, output_file, progress_callback=cli_progress)
    except Exception as e:
        print(f"错误: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

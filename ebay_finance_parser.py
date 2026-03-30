import os
import re
from datetime import datetime, timedelta

import pandas as pd


OUTPUT_COLUMNS = [
    '站点',
    '平台',
    '店铺号',
    '店铺公司',
    '归属月份',
    '来源数据报告',
    '归属',
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

HEADER_KEYWORDS = {
    'payment_summary': {'付款', 'Payments'},
    'debits': {'借额', 'Debits'},
    'subtotal': {'小计', 'Subtotal'},
}

CATEGORY_LABEL_TO_KEY = {
    '订单': 'income',
    'Orders': 'income',
    '退款': 'income',
    'Refunds': 'income',
    '支出': 'Expenses',
    'Expenses': 'Expenses',
    '净转账': 'Transfers',
    'Net transfers': 'Transfers',
    '调整': 'Adjustments',
    'Adjustments': 'Adjustments',
}

MONTH_NAME_MAP = {
    'Jan': 1,
    'Feb': 2,
    'Mar': 3,
    'Apr': 4,
    'May': 5,
    'Jun': 6,
    'Jul': 7,
    'Aug': 8,
    'Sep': 9,
    'Oct': 10,
    'Nov': 11,
    'Dec': 12,
}


def matches_keyword(text, keyword_key):
    return str(text or '').strip() in HEADER_KEYWORDS.get(keyword_key, set())


def parse_amount(text):
    if not text:
        return None
    text = str(text).strip()
    if re.match(r'^-?(?:US\$|\$|¥)[\d,]+\.?\d*$', text):
        num_str = re.sub(r'(US\$|\$|¥|,)', '', text)
        try:
            return float(num_str)
        except ValueError:
            return None
    return None


def parse_report_month_text(month_value):
    month_text = str(month_value or '').strip()
    if not month_text:
        return ''

    month_match = re.match(r'^(\d{4})-(\d{1,2})$', month_text)
    if month_match:
        return f"{int(month_match.group(1))}年{int(month_match.group(2))}月"

    chinese_match = re.match(r'^(\d{4})年(\d{1,2})月$', month_text)
    if chinese_match:
        return f"{int(chinese_match.group(1))}年{int(chinese_match.group(2))}月"

    return ''


def build_exchange_rate_map(exchange_rate_entries):
    rates = {}
    for entry in exchange_rate_entries or []:
        month = parse_report_month_text((entry or {}).get('month'))
        raw_rate = (entry or {}).get('rate')
        if not month or raw_rate in ('', None):
            continue
        try:
            rates[month] = float(raw_rate)
        except (TypeError, ValueError):
            continue
    return rates


def detect_report_month(text, filename):
    filename = os.path.basename(filename)
    match = re.search(r'_(\d{4})-(\d{2})-\d{2}_(\d{4})-(\d{2})-\d{2}_', filename)
    if match:
        return f"{int(match.group(1))}年{int(match.group(2))}月"

    chinese_match = re.search(r'日期范围:\s*(\d{4})年(\d{1,2})月\d{1,2}日-\d{4}年\d{1,2}月\d{1,2}日', text)
    if chinese_match:
        return f"{int(chinese_match.group(1))}年{int(chinese_match.group(2))}月"

    english_match = re.search(r'Date range:\s*([A-Za-z]{3}) \d{1,2}, (\d{4})-[A-Za-z]{3} \d{1,2}, \d{4}', text)
    if english_match:
        return f"{int(english_match.group(2))}年{MONTH_NAME_MAP[english_match.group(1)]}月"

    return ''


def extract_file_info(filename):
    base_name = os.path.splitext(os.path.basename(filename))[0]
    match = re.match(r'^(.*?)_(\d{4})-(\d{2})-\d{2}_(\d{4})-(\d{2})-\d{2}_财务报告$', base_name)
    if match:
        shop_id = match.group(1)
        month_str = f"{int(match.group(2))}年{int(match.group(3))}月"
        return shop_id, month_str
    return base_name, ''


def parse_pdf_dynamic(pdf_path):
    try:
        import fitz
    except ImportError as exc:
        raise ImportError('缺少 PyMuPDF 依赖，请先安装 fitz/PyMuPDF') from exc

    doc = fitz.open(pdf_path)
    try:
        full_text = '\n'.join(page.get_text() for page in doc)
    finally:
        doc.close()

    lines = [line.strip() for line in full_text.split('\n')]
    results = []
    current_category = None
    skip_keywords = {
        '借额', '退款', '净价', '小计', '付款', '财务概览', '交易摘要', '日期范围', '已生成', '', '费用',
        'Debits', 'Credits', 'Net', 'Subtotal', 'Payments', 'Financial overview',
        'Transactions summary', 'Date range', 'Generated', 'Fees',
    }

    i = 0
    while i < len(lines):
        line = lines[i]

        if matches_keyword(line, 'payment_summary'):
            current_category = None
            i += 1
            continue

        if line in CATEGORY_LABEL_TO_KEY and i + 1 < len(lines):
            next_line = lines[i + 1]
            if matches_keyword(next_line, 'debits'):
                current_category = CATEGORY_LABEL_TO_KEY[line]
                i += 1
                continue

        if matches_keyword(line, 'subtotal'):
            if current_category == 'income' and not any(item['item'] in {'订单', 'Orders'} for item in results):
                amounts = []
                j = i + 1
                while j < len(lines) and len(amounts) < 3:
                    amount = parse_amount(lines[j])
                    if amount is not None:
                        amounts.append(amount)
                    j += 1

                if len(amounts) == 3:
                    results.append({
                        'item': '订单' if any('订单' in current_line for current_line in lines[:20]) else 'Orders',
                        'category': 'income',
                        'profit_item': '主营业务收入',
                        'net_value': amounts[2],
                    })
                    i = j
                    continue

            elif current_category == 'Adjustments':
                amounts = []
                j = i + 1
                while j < len(lines) and len(amounts) < 3:
                    amount = parse_amount(lines[j])
                    if amount is not None:
                        amounts.append(amount)
                    j += 1

                if len(amounts) == 3:
                    results.append({
                        'item': '调整' if any('调整' in current_line for current_line in lines[:120]) else 'Adjustments',
                        'category': 'Adjustments',
                        'profit_item': '调整项',
                        'net_value': amounts[2],
                    })
                    i = j
                    continue

        if line in skip_keywords:
            i += 1
            continue

        if current_category is None and parse_amount(line) is not None:
            i += 1
            continue

        if current_category and line and parse_amount(line) is None:
            amounts = []
            j = i + 1
            attempts = 0
            while j < len(lines) and len(amounts) < 3 and attempts < 6:
                amount = parse_amount(lines[j])
                if amount is not None:
                    amounts.append(amount)
                elif lines[j] and lines[j] not in skip_keywords and parse_amount(lines[j]) is None:
                    break
                j += 1
                attempts += 1

            if len(amounts) == 3:
                item_name = line
                profit_item = '销售费用'
                if current_category == 'income':
                    lower_name = item_name.lower()
                    if '索赔' in item_name or '付款纠纷' in item_name or 'claim' in lower_name or 'dispute' in lower_name:
                        profit_item = '其他业务收入'
                    else:
                        profit_item = '主营业务收入'
                elif current_category == 'Transfers':
                    profit_item = '应收账款'
                elif current_category == 'Adjustments':
                    profit_item = '调整项'

                results.append({
                    'item': item_name,
                    'category': current_category,
                    'profit_item': profit_item,
                    'net_value': amounts[2],
                })
                i = j
                continue

        i += 1

    return results, full_text


def process_ebay_finance_folder(
    input_folder,
    output_file,
    exchange_rate_entries=None,
    store_name_override='',
    progress_callback=None,
):
    if not os.path.isdir(input_folder):
        raise ValueError(f'输入文件夹不存在: {input_folder}')

    if not output_file.endswith('.xlsx'):
        output_file += '.xlsx'

    pdf_files = sorted(file_name for file_name in os.listdir(input_folder) if file_name.lower().endswith('.pdf'))
    if not pdf_files:
        raise ValueError('文件夹中没有PDF文件')

    exchange_rates = build_exchange_rate_map(exchange_rate_entries)
    if not exchange_rates:
        raise ValueError('请先配置至少一条汇率')

    all_rows = []
    error_rows = []
    success_count = 0
    store_name_override = str(store_name_override or '').strip()

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

    for index, pdf_file in enumerate(pdf_files, 1):
        pdf_path = os.path.join(input_folder, pdf_file)
        try:
            parsed_shop_name, month_str = extract_file_info(pdf_file)
            items, full_text = parse_pdf_dynamic(pdf_path)
            if not month_str:
                month_str = detect_report_month(full_text, pdf_file)
            if not month_str:
                raise ValueError('无法识别归属月份')

            rate = exchange_rates.get(month_str)
            if rate is None:
                raise ValueError(f'未找到 {month_str} 对应汇率，请先在页面维护该月份汇率')

            if not items:
                raise ValueError('未识别到可导出的财务明细')

            final_shop_name = store_name_override or parsed_shop_name

            for item in items:
                all_rows.append({
                    '站点': '美国',
                    '平台': 'ebay',
                    '店铺号': final_shop_name,
                    '店铺公司': '',
                    '归属月份': month_str,
                    '来源数据报告': '财务报告',
                    '归属': item['category'],
                    '解释': item['item'],
                    '利润表项目': item['profit_item'],
                    '原币金额': item['net_value'],
                    '汇率': rate,
                    '换算本币（人民币）': round(item['net_value'] * rate, 6),
                })

            success_count += 1
            last_result = {
                'index': index,
                'file': pdf_file,
                'status': 'success',
                'row_count': len(items),
                'month': month_str,
                'shop_name': final_shop_name,
            }
        except Exception as exc:
            error_rows.append({
                '文件名': pdf_file,
                '阶段': 'process_ebay_finance_pdf',
                '原因': str(exc),
            })
            last_result = {
                'index': index,
                'file': pdf_file,
                'status': 'error',
                'reason': str(exc),
            }

        if progress_callback:
            progress_callback({
                'stage': 'processing',
                'total_files': len(pdf_files),
                'processed_files': index,
                'success_count': success_count,
                'failure_count': len(error_rows),
                'current_file': pdf_file,
                'last_result': last_result,
                'errors': error_rows[-20:],
            })

    if not all_rows and error_rows:
        raise ValueError('没有成功解析任何PDF文件')

    df = pd.DataFrame(all_rows).reindex(columns=OUTPUT_COLUMNS)
    error_df = pd.DataFrame(error_rows).reindex(columns=ERROR_COLUMNS)
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='ebay')
        if not error_df.empty:
            error_df.to_excel(writer, index=False, sheet_name='Errors')

    result = {
        'total_files': len(pdf_files),
        'success_count': success_count,
        'failure_count': len(error_rows),
        'skipped_count': 0,
        'total_rows': len(all_rows),
        'output_file': output_file,
        'errors': error_rows,
        'skipped': [],
    }

    if progress_callback:
        progress_callback({
            'stage': 'completed',
            **result,
        })

    return result

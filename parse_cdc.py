#!/usr/bin/env python3
"""
CDC Statement Parser (DHS-1381)
Extracts child payment data from Michigan CDC Statement of Payments PDFs.
Called from Node.js via child_process.
Usage: python3 parse_cdc.py <pdf_path>
Output: JSON to stdout
"""

import sys
import json
import re
import pdfplumber

def parse_cdc_statement(pdf_path):
    """Parse a DHS-1381 CDC Statement of Payments PDF."""
    
    result = {
        'voucher': '',
        'voucher_date': '',
        'provider_id': '',
        'provider_name': '',
        'provider_address': '',
        'center': '',
        'pay_period': '',
        'statement_date': '',
        'total_pay': 0,
        'net_total_pay': 0,
        'children': [],
        'errors': []
    }
    
    try:
        full_text = ''
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    full_text += text + '\n'
        
        if not full_text.strip():
            result['errors'].append('Could not extract text from PDF')
            return result
        
        # Extract header info
        date_match = re.search(r'Date:\s*(\d{2}/\d{2}/\d{4})', full_text)
        if date_match:
            result['statement_date'] = date_match.group(1)
            result['voucher_date'] = date_match.group(1)
        
        voucher_match = re.search(r'(BFI\d+)', full_text)
        if voucher_match:
            result['voucher'] = voucher_match.group(1)
        
        provider_match = re.search(r'Provider ID\s+(\d+)', full_text)
        if not provider_match:
            provider_match = re.search(r'BFI\d+\s+\d{2}/\d{2}/\d{4}\s+(\d+)', full_text)
        if provider_match:
            result['provider_id'] = provider_match.group(1)
        
        # Detect center from provider name/address
        if 'PEACE' in full_text.upper():
            result['center'] = 'Peace Boulevard'
            result['provider_name'] = "THE CHILDREN'S CENTER - PEACE BOULEVARD"
        elif 'MONTESSORI' in full_text.upper():
            result['center'] = 'Montessori'
            result['provider_name'] = "MONTESSORI CHILDREN'S CENTER OF ST. JOSEPH"
        elif 'NILES' in full_text.upper() or '210 E MAIN' in full_text.upper():
            result['center'] = 'Niles'
            result['provider_name'] = "THE CHILDREN'S CENTER - NILES"
        
        # Extract total pay
        total_match = re.search(r'(?:Net\s+)?Total\s+Pay\s+\$\s*([\d,]+\.?\d*)', full_text)
        if total_match:
            result['total_pay'] = float(total_match.group(1).replace(',', ''))
        
        net_match = re.search(r'Net\s+Total\s+Pay\s+\$\s*([\d,]+\.?\d*)', full_text)
        if net_match:
            result['net_total_pay'] = float(net_match.group(1).replace(',', ''))
        
        # Parse each child entry
        # Pattern: "Child's Name:" or "Child's Name :" followed by the name
        child_pattern = re.compile(
            r"Child's Name\s*:\s*(.+?)(?:\s+Case No\.\s*:\s*(\d+))?\s+Child's ID No\.\s*:\s*(\d+)",
            re.IGNORECASE
        )
        
        # Split text into child blocks
        # Each block starts with "Child's Name" and ends before the next one or at end
        child_splits = re.split(r"(?=Child's Name\s*:)", full_text, flags=re.IGNORECASE)
        
        for block in child_splits:
            if not block.strip() or "Child's Name" not in block:
                continue
            
            # Extract child info
            name_match = re.search(
                r"Child's Name\s*:\s*(.+?)(?:\s+Case No\.?\s*:?\s*(\d+))?\s+Child's ID No\.?\s*:?\s*(\d+)",
                block, re.IGNORECASE
            )
            
            if not name_match:
                continue
            
            child_name_raw = name_match.group(1).strip()
            case_no = name_match.group(2) or ''
            child_id = name_match.group(3) or ''
            
            # Clean up child name (remove trailing "Case No." if partially captured)
            child_name = re.sub(r'\s*Case No\.?\s*$', '', child_name_raw).strip()
            
            # Extract pay period lines
            # Pattern: 6XX (MM/DD/YYYY - MM/DD/YYYY) followed by numbers
            pay_lines = re.findall(
                r'(\d{3})\s+\((\d{2}/\d{2}/\d{4})\s*-\s*(\d{2}/\d{2}/\d{4})\)\s*(.*?)(?=\n|Client Recoupment)',
                block
            )
            
            # Also catch lines where hours auth is 0 and shows as "$ 0.00 $ 0.00 Error"
            pay_lines_alt = re.findall(
                r'(\d{3})\s+\((\d{2}/\d{2}/\d{4})\s*-\s*(\d{2}/\d{2}/\d{4})\)\s*\$?\s*(.*?)(?=\n|Client)',
                block
            )
            
            total_amount_paid = 0
            hours_billed = 0
            hours_paid = 0
            hours_auth = 0
            max_rate = 0
            fc = 0
            error_desc = ''
            pay_period_str = ''
            line_details = []
            
            lines_to_parse = pay_lines if pay_lines else pay_lines_alt
            
            for line in lines_to_parse:
                period_code = line[0]
                period_start = line[1]
                period_end = line[2]
                rest = line[3].strip()
                
                pay_period_str = f"{period_start} - {period_end}"
                
                # Parse the numbers from the rest of the line
                # Could be: "90 86 86 90 $ 0 $ 6.30 $ 567.00"
                # Or: "0 60 0 0 $ 0 $ 6.15 $ 0.00 No Authorization"
                # Or: "$ 0.00 $ 0.00 No Authorization" (when hours auth is 0)
                
                # Extract all dollar amounts
                amounts = re.findall(r'\$\s*([\d,]+\.?\d*)', rest)
                
                # Extract numbers before dollar signs (hours)
                nums_before = re.match(r'^([\d\s]+?)(?:\$|$)', rest)
                hours_nums = []
                if nums_before:
                    hours_nums = [int(x) for x in nums_before.group(1).split() if x.strip().isdigit()]
                
                # Parse hours
                line_hours_auth = hours_nums[0] if len(hours_nums) >= 1 else 0
                line_hours_billed = hours_nums[1] if len(hours_nums) >= 2 else 0
                line_hours_paid = hours_nums[2] if len(hours_nums) >= 3 else 0
                line_block_hours = hours_nums[3] if len(hours_nums) >= 4 else 0
                
                hours_auth = max(hours_auth, line_hours_auth)
                hours_billed += line_hours_billed
                hours_paid += line_hours_paid
                
                # Parse dollar amounts
                if len(amounts) >= 3:
                    fc = max(fc, float(amounts[0].replace(',', '')))
                    max_rate = max(max_rate, float(amounts[1].replace(',', '')))
                    line_amount = float(amounts[2].replace(',', ''))
                    total_amount_paid += line_amount
                elif len(amounts) >= 2:
                    # Might be "$ 0.00 $ 0.00" format
                    total_amount_paid += float(amounts[-1].replace(',', ''))
                elif len(amounts) == 1:
                    total_amount_paid += float(amounts[0].replace(',', ''))
                
                # Check for error description
                error_patterns = [
                    'No Authorization',
                    'Duplicate Bill',
                    'Total billing hours',
                    'Hours billed greater',
                    'More than 10 absences',
                ]
                for ep in error_patterns:
                    if ep.lower() in rest.lower() or ep.lower() in block.lower():
                        if ep == 'Total billing hours':
                            error_desc = 'Total billing hours exceeds Max hours'
                        elif ep == 'Hours billed greater':
                            error_desc = 'Hours billed greater than authorized'
                        elif ep == 'More than 10 absences':
                            error_desc = 'More than 10 absences in a row'
                        else:
                            error_desc = ep
                        break
                
                line_details.append({
                    'period_code': period_code,
                    'period_start': period_start,
                    'period_end': period_end,
                    'hours_auth': line_hours_auth,
                    'hours_billed': line_hours_billed,
                    'hours_paid': line_hours_paid,
                    'amount': line_amount if len(amounts) >= 3 else 0
                })
            
            # Also check for error in the broader block if not found in pay lines
            if not error_desc:
                for ep in ['No Authorization', 'Duplicate Bill']:
                    if ep in block:
                        error_desc = ep
                        break
            
            # Set pay period from first line if not set at result level
            if pay_period_str and not result['pay_period']:
                result['pay_period'] = pay_period_str
            
            child_entry = {
                'name': child_name,
                'name_normalized': normalize_name(child_name),
                'case_no': case_no,
                'child_id': child_id,
                'pay_period': pay_period_str,
                'hours_auth': hours_auth,
                'hours_billed': hours_billed,
                'hours_paid': hours_paid,
                'fc': fc,
                'max_rate': max_rate,
                'amount_paid': round(total_amount_paid, 2),
                'error': error_desc,
                'is_paid': total_amount_paid > 0,
                'is_no_auth': error_desc == 'No Authorization',
                'is_duplicate': error_desc == 'Duplicate Bill',
                'line_details': line_details
            }
            
            result['children'].append(child_entry)
        
    except Exception as e:
        result['errors'].append(str(e))
    
    return result


def normalize_name(name):
    """Normalize a child name for matching against Playground records."""
    # Remove middle names/initials, convert to lowercase
    name = name.strip().upper()
    parts = name.split()
    if len(parts) >= 2:
        # Take first and last
        first = parts[0]
        last = parts[-1]
        return f"{first} {last}".lower()
    return name.lower()


def main():
    if len(sys.argv) < 2:
        print(json.dumps({'error': 'Usage: parse_cdc.py <pdf_path>'}))
        sys.exit(1)
    
    pdf_path = sys.argv[1]
    result = parse_cdc_statement(pdf_path)
    
    # Summary stats
    result['summary'] = {
        'total_children': len(result['children']),
        'children_paid': len([c for c in result['children'] if c['is_paid']]),
        'children_no_auth': len([c for c in result['children'] if c['is_no_auth']]),
        'children_duplicate': len([c for c in result['children'] if c['is_duplicate']]),
        'total_paid': round(sum(c['amount_paid'] for c in result['children']), 2),
        'total_unpaid': len([c for c in result['children'] if not c['is_paid'] and not c['is_duplicate']])
    }
    
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()

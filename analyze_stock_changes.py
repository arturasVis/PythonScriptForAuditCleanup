import pandas as pd
import numpy as np
from datetime import datetime
from collections import defaultdict

def calculate_purchase_price(stock_value, stock_level, default_pp=0):
    if stock_level == 0:
        return default_pp
    calculated_pp = stock_value / stock_level
    return default_pp if calculated_pp == 0 else calculated_pp

# Initialize stock tracker from AutoImport.csv
print("Reading initial stock levels from AutoImport.csv...")
initial_stock_df = pd.read_csv('AutoImport.csv')
stock_tracker = {}

# Initialize stock_tracker with data from AutoImport.csv
for _, row in initial_stock_df.iterrows():
    sku = row['SKU']
    location = row['Stock Location']
    # Remove commas before converting to float
    stock_level = float(str(row['Stock level at location']).replace(',', ''))
    stock_value = float(str(row['Stock value at location']).replace(',', ''))
    default_purchase_price = float(str(row['Purchase Price']).replace(',', ''))
    
    if sku not in stock_tracker:
        stock_tracker[sku] = {}
    
    stock_tracker[sku][location] = {
        'level': stock_level,
        'value': stock_value,
        'purchase_price': calculate_purchase_price(stock_value, stock_level, default_purchase_price)
    }

print(f"Initialized stock_tracker with {len(stock_tracker)} SKUs from AutoImport.csv")

# Read the CSV file in chunks and sort by date
chunk_size = 10000
all_chunks = []
for chunk in pd.read_csv('QueryData-30-05-25(12_16_13).csv', chunksize=chunk_size):
    chunk['StockChangeDateTime'] = pd.to_datetime(chunk['StockChangeDateTime'], format='%d/%m/%Y %H:%M:%S')
    all_chunks.append(chunk)

# Combine and sort all chunks by date
df = pd.concat(all_chunks)
df = df.sort_values('StockChangeDateTime')

# Process each row chronologically
for _, row in df.iterrows():
    sku = row['ItemNumber']
    location = row['Location']
    change_qty = float(row['ChangeQTY'])
    change_value = float(row['ChangeValue'])
    change_source = row['ChangeSource']
    date_time = row['StockChangeDateTime']

    # Only debug print for specific SKU and location
    debug_print = (sku == 'SCRAP-RAM-NONMETAL' and location == 'Default')
    
    if debug_print:
        print(f"\n{'='*80}")
        print(f"Processing operation at {date_time}")
        print(f"SKU: {sku}, Location: {location}")
        print(f"Change Source: {change_source}")
        print(f"Change QTY: {change_qty}, Change Value: {change_value}")

    # Initialize location if not exists
    if location not in stock_tracker.get(sku, {}):
        if sku not in stock_tracker:
            stock_tracker[sku] = {}
        stock_tracker[sku][location] = {
            'level': 0,
            'value': 0,
            'purchase_price': 0
        }
        if debug_print:
            print("Initialized new SKU/Location combination")

    current = stock_tracker[sku][location]
    if debug_print:
        print(f"Before operation - Level: {current['level']}, Value: {current['value']}, PP: {current['purchase_price']}")

    # Skip "Imported from file" operations
    if 'Imported from file' in change_source:
        if debug_print:
            print("Skipping Imported from file operation")
        continue

    # Handle PO operations
    if 'PO' in change_source:
        if debug_print:
            print("Processing PO operation")
        # Add the PO values to current totals
        current['level'] += change_qty
        current['value'] += change_value
        # Recalculate purchase price
        if current['level'] != 0:
            current['purchase_price'] = current['value'] / current['level']
        if debug_print:
            print(f"After PO - Level: {current['level']}, Value: {current['value']}, PP: {current['purchase_price']}")
    else:
        # For non-PO operations, use purchase price to calculate value
        if current['purchase_price'] > 0:
            if debug_print:
                print("Processing non-PO operation with existing purchase price")
            # Calculate value based on purchase price
            operation_value = change_qty * current['purchase_price']
            current['level'] += change_qty
            current['value'] += operation_value
            if debug_print:
                print(f"Calculated operation value: {operation_value}")
                print(f"After operation - Level: {current['level']}, Value: {current['value']}, PP: {current['purchase_price']}")
        elif current['purchase_price'] == 0:
            if debug_print:
                print("Processing non-PO operation with no purchase price")
            # If no purchase price yet, just track the level
            current['level'] += change_qty
            current['value'] += change_value
            if current['level'] != 0:
                current['purchase_price'] = current['value'] / current['level']
            if debug_print:
                print(f"After operation - Level: {current['level']}, Value: {current['value']}, PP: {current['purchase_price']}")

# Create summary DataFrame directly from stock_tracker
summary_data = []
for sku, locations in stock_tracker.items():
    for location, data in locations.items():
        summary_data.append({
            'ItemNumber': sku,
            'Location': location,
            'FinalStockLevel': data['level'],
            'FinalStockValue': data['value'],
            'PurchasePrice': data['purchase_price']
        })

summary_df = pd.DataFrame(summary_data)

# Save summary to CSV
summary_df.to_csv('stock_summary.csv', index=False)

# Print summary statistics
print("\nSummary Statistics:")
print(f"Total unique SKUs: {len(stock_tracker)}")
print(f"Total locations: {sum(len(locations) for locations in stock_tracker.values())}")
print("\nSample of final stock levels:")
print(summary_df.head()) 
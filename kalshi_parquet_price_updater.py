import sys
import subprocess
import importlib
import queue
import threading
import json
import datetime
import asyncio
import websockets
import base64
import csv
import os
import pandas as pd
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding, rsa

# --- Package Installation Helper ---
def install_package(package):
    try:
        if package == 'PyQt5': importlib.import_module('PyQt5.QtWidgets')
        else: importlib.import_module(package.replace('-', '_'))
    except ImportError:
        print(f"Installing {package}..."); subprocess.check_call([sys.executable, "-m", "pip", "install", package]); print(f"{package} installed.")

# --- Import Required Libraries ---
try:
    required_packages = ['PyQt5', 'pandas', 'pyarrow']
    for package in required_packages: install_package(package)
    from PyQt5.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QTableView, QHeaderView, QMessageBox, QLabel, QPushButton, QDialog, QFrame, QTextEdit, QProgressBar)
    from PyQt5.QtCore import (QAbstractTableModel, Qt, QVariant, QModelIndex, QThread, pyqtSignal, QTimer)
    from PyQt5.QtGui import QColor, QFont
except Exception as e:
    print(f"FATAL ERROR: Could not import required packages. Details: {e}"); sys.exit(1)

# --- Global Variables ---
trade_data_queue = queue.Queue()
market_data_queue = queue.Queue()
trade_history = {}  # Store previous prices for each market
parquet_data = None  # Store the parquet export data
parquet_tickers = set()  # Set of tickers from parquet file
updated_tickers = set()  # Track which tickers have been updated
csv_file_path = f"parquet_price_updates_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

# --- Load Parquet Export Data ---
def load_parquet_data():
    """Load the parquet export data and extract tickers"""
    global parquet_data, parquet_tickers
    
    try:
        parquet_file_path = 
        
        if not os.path.exists(parquet_file_path):
            print(f"FATAL: Parquet export file not found at {parquet_file_path}")
            return False
        
        print(f"Loading parquet data from: {parquet_file_path}")
        parquet_data = pd.read_csv(parquet_file_path)
        
        if 'ticker' not in parquet_data.columns:
            print("FATAL: 'ticker' column not found in parquet export file")
            return False
        
        # Filter out finalized markets
        if 'status' in parquet_data.columns:
            # Keep only non-finalized markets
            active_data = parquet_data[parquet_data['status'] != 'finalized'].copy()
            finalized_count = len(parquet_data) - len(active_data)
            print(f"Filtered out {finalized_count} finalized markets")
            parquet_data = active_data
        else:
            print("Warning: 'status' column not found, showing all tickers")
        
        parquet_tickers = set(parquet_data['ticker'].dropna().unique())
        print(f"Loaded {len(parquet_tickers)} active tickers from parquet file")
        
        return True
        
    except Exception as e:
        print(f"Error loading parquet data: {e}")
        return False

# --- CSV File Setup ---
def initialize_csv_file():
    """Initialize the CSV file with headers"""
    headers = [
        'timestamp', 'market_ticker', 'market_title', 'side', 'trade_type', 'price', 'quantity', 
        'price_change', 'price_change_percent', 'volume_change', 'total_volume',
        'yes_bid', 'yes_ask', 'no_bid', 'no_ask', 'last_price', 'parquet_updated'
    ]
    
    with open(csv_file_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(headers)
    
    print(f"CSV file initialized: {csv_file_path}")

def record_trade_to_csv(trade_data):
    """Record a trade to the CSV file by adding it to the top"""
    try:
        # Read existing data
        existing_data = []
        try:
            with open(csv_file_path, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.reader(csvfile)
                headers = next(reader)  # Skip header
                existing_data = list(reader)
        except FileNotFoundError:
            # File doesn't exist yet, that's okay
            pass
        
        # Prepare new trade row
        new_row = [
            trade_data['timestamp'],
            trade_data['market_ticker'],
            trade_data['market_title'],
            trade_data['side'],
            trade_data['trade_type'],
            trade_data['price'],
            trade_data['quantity'],
            trade_data['price_change'],
            trade_data['price_change_percent'],
            trade_data['volume_change'],
            trade_data['total_volume'],
            trade_data['yes_bid'],
            trade_data['yes_ask'],
            trade_data['no_bid'],
            trade_data['no_ask'],
            trade_data['last_price'],
            trade_data['parquet_updated']
        ]
        
        # Write back to file with new trade at top
        with open(csv_file_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            # Write header
            writer.writerow([
                'timestamp', 'market_ticker', 'market_title', 'side', 'trade_type', 'price', 'quantity', 
                'price_change', 'price_change_percent', 'volume_change', 'total_volume',
                'yes_bid', 'yes_ask', 'no_bid', 'no_ask', 'last_price', 'parquet_updated'
            ])
            # Write new trade first
            writer.writerow(new_row)
            # Write existing data
            for row in existing_data:
                writer.writerow(row)
                
    except Exception as e:
        print(f"Error writing to CSV: {e}")

def update_parquet_data(trade_data):
    """Update the parquet data with the actual incoming trade price"""
    global parquet_data, updated_tickers
    
    try:
        ticker = trade_data['market_ticker']
        
        if ticker not in parquet_tickers:
            return False
        
        # Find the row in parquet data that matches this ticker
        mask = parquet_data['ticker'] == ticker
        if not mask.any():
            return False
        
        row_idx = mask.idxmax()
        side = trade_data['side']
        
        # Use the actual incoming trade price from the websocket
        incoming_price = trade_data['price']
        
        # Update the correct price column based on side
        if side == 'YES':
            # For YES trades, update yes_bid and yes_ask with the actual incoming price
            parquet_data.loc[row_idx, 'yes_bid'] = incoming_price
            parquet_data.loc[row_idx, 'yes_ask'] = incoming_price
            parquet_data.loc[row_idx, 'last_price'] = incoming_price
            
        elif side == 'NO':
            # For NO trades, update no_bid and no_ask with the actual incoming price
            parquet_data.loc[row_idx, 'no_bid'] = incoming_price
            parquet_data.loc[row_idx, 'no_ask'] = incoming_price
            parquet_data.loc[row_idx, 'last_price'] = incoming_price
        
        # Mark as updated
        updated_tickers.add(ticker)
        
        print(f"Updated parquet data for ticker: {ticker} ({side} side) - Used incoming price: {incoming_price}")
        return True
        
    except Exception as e:
        print(f"Error updating parquet data: {e}")
        return False

def save_updated_parquet():
    """Save the updated parquet data to a new file"""
    try:
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = f"parquet_export_updated_{timestamp}.csv"
        
        # Save with all original columns preserved
        parquet_data.to_csv(output_file, index=False)
        print(f"Updated parquet data saved to: {output_file}")
        
        return output_file
        
    except Exception as e:
        print(f"Error saving updated parquet data: {e}")
        return None

# --- Backend WebSocket and Cryptography Logic ---
def load_private_key_from_file(file_path):
    with open(file_path, "rb") as key_file: return serialization.load_pem_private_key(key_file.read(), password=None)

def sign_pss_text(private_key: rsa.RSAPrivateKey, text: str) -> str:
    message = text.encode('utf-8')
    signature = private_key.sign(message, padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH), hashes.SHA256())
    return base64.b64encode(signature).decode('utf-8')

async def send_keepalive(websocket):
    """Sends an application-level ping every 20 seconds."""
    try:
        while True:
            try:
                await websocket.send(json.dumps({"id": 99, "cmd": "ping"}))
                await asyncio.sleep(20)
            except websockets.exceptions.ConnectionClosed:
                print("Keep-alive detected connection closed")
                break
            except Exception as e:
                print(f"Keep-alive error: {e}")
                break
    except Exception as e:
        print(f"Keep-alive task error: {e}")

async def receive_messages(websocket):
    """Receives messages from the WebSocket and processes trades"""
    try:
        async for message in websocket:
            try:
                data = json.loads(message)
                if data.get('type') == 'trade' and 'msg' in data:
                    trade_data = data['msg']
                    processed_trade = process_trade_data(trade_data)
                    if processed_trade:
                        trade_data_queue.put(processed_trade)
                        market_data_queue.put(processed_trade)
            except json.JSONDecodeError:
                print("Received invalid JSON message")
                continue
            except Exception as e:
                print(f"Error processing message: {e}")
                continue
    except websockets.exceptions.ConnectionClosed:
        print("Message receiver detected connection closed")
    except Exception as e:
        print(f"Message receiver error: {e}")

def process_trade_data(trade_data):
    """Process trade data and calculate price changes"""
    try:
        ticker = trade_data.get('market_ticker')
        if not ticker:
            return None
        
        # Only process tickers that exist in the parquet file
        if ticker not in parquet_tickers:
            return None
        
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        side = trade_data.get('taker_side', 'N/A').upper()
        quantity = trade_data.get('count', 0)
        
        # Determine price based on side
        if side == 'YES':
            price = trade_data.get('yes_price')
        elif side == 'NO':
            price = trade_data.get('no_price')
        else:
            price = None
        
        # Determine trade type from websocket data
        # Look for maker_side vs taker_side to determine buy/sell
        maker_side = trade_data.get('maker_side', '').upper()
        taker_side = trade_data.get('taker_side', '').upper()
        
        trade_type = 'UNKNOWN'
        if maker_side and taker_side:
            # If taker_side is YES and maker_side is NO, taker is buying YES
            # If taker_side is NO and maker_side is YES, taker is buying NO
            if taker_side == 'YES' and maker_side == 'NO':
                trade_type = 'BUY'  # Taker is buying YES
            elif taker_side == 'NO' and maker_side == 'YES':
                trade_type = 'BUY'  # Taker is buying NO
            elif taker_side == 'YES' and maker_side == 'YES':
                trade_type = 'SELL'  # Taker is selling YES
            elif taker_side == 'NO' and maker_side == 'NO':
                trade_type = 'SELL'  # Taker is selling NO
        
        # Fallback: if we can't determine from maker/taker, use price comparison
        if trade_type == 'UNKNOWN' and price is not None:
            mask = parquet_data['ticker'] == ticker
            if mask.any():
                row_idx = mask.idxmax()
                if side == 'YES':
                    yes_bid = parquet_data.loc[row_idx, 'yes_bid']
                    yes_ask = parquet_data.loc[row_idx, 'yes_ask']
                    if not pd.isna(yes_ask) and abs(price - yes_ask) < 0.01:
                        trade_type = 'BUY'
                    elif not pd.isna(yes_bid) and abs(price - yes_bid) < 0.01:
                        trade_type = 'SELL'
                elif side == 'NO':
                    no_bid = parquet_data.loc[row_idx, 'no_bid']
                    no_ask = parquet_data.loc[row_idx, 'no_ask']
                    if not pd.isna(no_ask) and abs(price - no_ask) < 0.01:
                        trade_type = 'BUY'
                    elif not pd.isna(no_bid) and abs(price - no_bid) < 0.01:
                        trade_type = 'SELL'
        
        # Calculate price changes
        price_change = 0
        price_change_percent = 0
        volume_change = quantity
        
        if ticker in trade_history:
            prev_data = trade_history[ticker]
            if price and prev_data.get('last_price'):
                price_change = price - prev_data['last_price']
                if prev_data['last_price'] != 0:
                    price_change_percent = (price_change / prev_data['last_price']) * 100
            volume_change = quantity
        
        # Update trade history
        trade_history[ticker] = {
            'last_price': price,
            'total_volume': trade_history.get(ticker, {}).get('total_volume', 0) + quantity,
            'last_update': timestamp
        }
        
        # Prepare trade record
        processed_trade = {
            'timestamp': timestamp,
            'market_ticker': ticker,
            'market_title': trade_data.get('market_title', 'N/A'),
            'side': side,
            'trade_type': trade_type,
            'price': price,
            'quantity': quantity,
            'price_change': round(price_change, 2) if price_change else 0,
            'price_change_percent': round(price_change_percent, 2) if price_change_percent else 0,
            'volume_change': volume_change,
            'total_volume': trade_history[ticker]['total_volume'],
            'yes_bid': trade_data.get('yes_bid'),
            'yes_ask': trade_data.get('yes_ask'),
            'no_bid': trade_data.get('no_bid'),
            'no_ask': trade_data.get('no_ask'),
            'last_price': price,
            'parquet_updated': False
        }
        
        # Update parquet data if this ticker exists
        if update_parquet_data(processed_trade):
            processed_trade['parquet_updated'] = True
        
        # Record to CSV
        record_trade_to_csv(processed_trade)
        
        return processed_trade
        
    except Exception as e:
        print(f"Error processing trade data: {e}")
        return None

async def connect_to_websocket():
    while True:
        try:
            private_key_path = r"C:\Users\cal3p\OneDrive\Documents\kalshi.pem"
            access_key = "97970105-48d7-4f7b-a7ea-44dbace20ded"
            websocket_url = "wss://api.elections.kalshi.com/trade-api/ws/v2"

            if not os.path.exists(private_key_path): 
                print(f"FATAL: Key not found at {private_key_path}")
                await asyncio.sleep(30)
                continue

            private_key = load_private_key_from_file(private_key_path)
            timestamp_str = str(int(datetime.datetime.now(datetime.UTC).timestamp() * 1000))
            msg_string = timestamp_str + "GET" + "/trade-api/ws/v2"
            sig = sign_pss_text(private_key, msg_string)
            headers = {"KALSHI-ACCESS-KEY": access_key, "KALSHI-ACCESS-SIGNATURE": sig, "KALSHI-ACCESS-TIMESTAMP": timestamp_str}

            async with websockets.connect(websocket_url, additional_headers=headers, ping_interval=None, ping_timeout=None) as websocket:
                print("WebSocket connection successful. Starting parquet price updates...")
                await websocket.send(json.dumps({"id": 1, "cmd": "subscribe", "params": {"channels": ["trade"]}}))
                
                keepalive_task = asyncio.create_task(send_keepalive(websocket))
                receiver_task = asyncio.create_task(receive_messages(websocket))
                
                try:
                    done, pending = await asyncio.wait(
                        [keepalive_task, receiver_task],
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    
                    for task in pending:
                        task.cancel()
                        try:
                            await task
                        except asyncio.CancelledError:
                            pass
                            
                except Exception as e:
                    print(f"Error in concurrent tasks: {e}")
                    keepalive_task.cancel()
                    receiver_task.cancel()

        except websockets.exceptions.ConnectionClosed as e:
            print(f"WebSocket connection closed: {e}")
        except websockets.exceptions.InvalidURI as e:
            print(f"Invalid WebSocket URI: {e}")
        except Exception as e:
            print(f"WebSocket session error: {e}")
        
        print("Connection dropped. Reconnecting in 5 seconds...")
        await asyncio.sleep(5)

def run_websocket():
    loop = asyncio.new_event_loop(); asyncio.set_event_loop(loop); loop.run_until_complete(connect_to_websocket())

# --- PyQt5 Frontend Classes ---

class ParquetUpdaterModel(QAbstractTableModel):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._data = []
        self._columns = ['Time', 'Ticker', 'Side', 'Trade Type', 'Price', 'Qty', 'Price Δ', 'Price Δ%', 'Volume Δ', 'Total Vol', 'Parquet Updated']
    
    def rowCount(self, parent=QModelIndex()): return len(self._data)
    def columnCount(self, parent=QModelIndex()): return len(self._columns)
    
    def data(self, index, role=Qt.DisplayRole):
        if not index.isValid(): return QVariant()
        row_data = self._data[index.row()]
        col_name = self._columns[index.column()]
        
        if role == Qt.DisplayRole:
            value = row_data.get(col_name, '')
            if col_name in ['Price Δ', 'Price Δ%'] and value != '':
                return f"{value:+.2f}" if col_name == 'Price Δ' else f"{value:+.2f}%"
            return str(value)
        
        if role == Qt.BackgroundRole:
            # Color entire row based on side for better visibility
            if row_data['Side'] == 'YES':
                return QColor('#1B5E20')  # Dark green for YES
            elif row_data['Side'] == 'NO':
                return QColor('#B71C1C')  # Dark red for NO
            elif col_name == 'Parquet Updated' and row_data.get(col_name, False):
                return QColor('#4CAF50')  # Green for updated
            elif col_name == 'Price Δ' and row_data.get(col_name, 0) > 0:
                return QColor('#2E7D32')  # Green for positive
            elif col_name == 'Price Δ' and row_data.get(col_name, 0) < 0:
                return QColor('#C62828')  # Red for negative
        
        if role == Qt.ForegroundRole:
            return QColor('white')
        
        return QVariant()
    
    def headerData(self, section, orientation, role=Qt.DisplayRole):
        if role == Qt.DisplayRole and orientation == Qt.Horizontal:
            return self._columns[section]
        return QVariant()
    
    def add_trade(self, trade_data):
        """Add a new trade to the top of the list"""
        new_row = {
            'Time': trade_data['timestamp'],
            'Ticker': trade_data['market_ticker'],
            'Side': trade_data['side'],
            'Trade Type': trade_data['trade_type'],
            'Price': trade_data['price'],
            'Qty': trade_data['quantity'],
            'Price Δ': trade_data['price_change'],
            'Price Δ%': trade_data['price_change_percent'],
            'Volume Δ': trade_data['volume_change'],
            'Total Vol': trade_data['total_volume'],
            'Parquet Updated': trade_data['parquet_updated']
        }
        
        self.beginInsertRows(QModelIndex(), 0, 0)
        self._data.insert(0, new_row)
        self.endInsertRows()
        
        # Keep only the last 1000 trades to prevent memory issues
        if len(self._data) > 1000:
            self.beginRemoveRows(QModelIndex(), 1000, len(self._data) - 1)
            self._data = self._data[:1000]
            self.endRemoveRows()

class QueueMonitorThread(QThread):
    new_trade_signal = pyqtSignal(dict)
    
    def run(self):
        while True:
            try:
                if not trade_data_queue.empty():
                    trade_data = trade_data_queue.get()
                    self.new_trade_signal.emit(trade_data)
                self.msleep(50)
            except Exception as e:
                print(f"Error in queue monitor: {e}")

class AutoSaveThread(QThread):
    save_complete_signal = pyqtSignal(str)
    
    def __init__(self, save_interval=300):  # Default 5 minutes
        super().__init__()
        self.save_interval = save_interval
        self.running = True
    
    def run(self):
        while self.running:
            try:
                self.sleep(self.save_interval)
                if self.running and parquet_data is not None and len(updated_tickers) > 0:
                    output_file = save_updated_parquet()
                    if output_file:
                        self.save_complete_signal.emit(output_file)
            except Exception as e:
                print(f"Error in auto-save thread: {e}")
    
    def stop(self):
        self.running = False

class TickerListDialog(QDialog):
    """Dialog to show the actual parquet export file content"""
    def __init__(self, parquet_data, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Parquet Export File Content")
        self.setGeometry(100, 100, 1400, 800)
        self.parquet_data = parquet_data
        
        layout = QVBoxLayout(self)
        
        # Title
        title = QLabel(f"Parquet Export File Content ({len(parquet_data)} rows, {len(parquet_data.columns)} columns):")
        title.setFont(QFont("Segoe UI", 12, QFont.Bold))
        layout.addWidget(title)
        
        # Create table view to display the data
        self.table_view = QTableView()
        self.table_view.setAlternatingRowColors(True)
        self.table_view.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)
        
        # Create model for the table
        self.model = PandasModel(parquet_data)
        self.table_view.setModel(self.model)
        
        layout.addWidget(self.table_view)
        
        # Close button
        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.accept)
        layout.addWidget(close_btn)
        
        self.setStyleSheet("""
            QDialog {
                background-color: #f8f9fa;
            }
            QLabel {
                color: #2c3e50;
                margin: 10px;
            }
            QTableView {
                background-color: white;
                border: 1px solid #ddd;
                border-radius: 5px;
                font-size: 9px;
            }
            QHeaderView::section {
                background-color: #e0e0e0;
                padding: 5px;
                border: 1px solid #ccc;
                font-weight: bold;
                font-size: 9px;
            }
            QPushButton {
                background-color: #2980b9;
                color: white;
                border: none;
                padding: 8px 16px;
                border-radius: 4px;
                font-weight: bold;
                margin: 10px;
            }
            QPushButton:hover {
                background-color: #3498db;
            }
        """)

    def update_price(self, ticker, side, new_price):
        """Update a specific price in the table and flash the cell"""
        try:
            # Find the row for this ticker
            mask = self.parquet_data['ticker'] == ticker
            if not mask.any():
                return
            
            row_idx = mask.idxmax()
            
            # Update the data
            if side == 'YES':
                self.parquet_data.loc[row_idx, 'yes_bid'] = new_price
                self.parquet_data.loc[row_idx, 'yes_ask'] = new_price
                self.parquet_data.loc[row_idx, 'last_price'] = new_price
                
                # Flash the updated columns
                yes_bid_col = self.parquet_data.columns.get_loc('yes_bid')
                yes_ask_col = self.parquet_data.columns.get_loc('yes_ask')
                last_price_col = self.parquet_data.columns.get_loc('last_price')
                
                self.model.flash_cell(row_idx, yes_bid_col)
                self.model.flash_cell(row_idx, yes_ask_col)
                self.model.flash_cell(row_idx, last_price_col)
                
            elif side == 'NO':
                self.parquet_data.loc[row_idx, 'no_bid'] = new_price
                self.parquet_data.loc[row_idx, 'no_ask'] = new_price
                self.parquet_data.loc[row_idx, 'last_price'] = new_price
                
                # Flash the updated columns
                no_bid_col = self.parquet_data.columns.get_loc('no_bid')
                no_ask_col = self.parquet_data.columns.get_loc('no_ask')
                last_price_col = self.parquet_data.columns.get_loc('last_price')
                
                self.model.flash_cell(row_idx, no_bid_col)
                self.model.flash_cell(row_idx, no_ask_col)
                self.model.flash_cell(row_idx, last_price_col)
            
            # Update the model data
            self.model.update_data(self.parquet_data)
            
        except Exception as e:
            print(f"Error updating price in dialog: {e}")

# Add PandasModel class for displaying the parquet data
class PandasModel(QAbstractTableModel):
    """Model for displaying pandas DataFrame in QTableView"""
    def __init__(self, data_frame, parent=None):
        super().__init__(parent)
        self._data_frame = data_frame
        self._updated_cells = set()  # Track which cells were recently updated
        self._flash_timer = QTimer()
        self._flash_timer.setSingleShot(True)
        self._flash_timer.timeout.connect(self.clear_flash)

    def rowCount(self, parent=QModelIndex()):
        return self._data_frame.shape[0]

    def columnCount(self, parent=QModelIndex()):
        return self._data_frame.shape[1]

    def data(self, index, role=Qt.DisplayRole):
        if not index.isValid():
            return QVariant()

        if role == Qt.DisplayRole:
            value = self._data_frame.iloc[index.row(), index.column()]
            if pd.isna(value):
                return ""
            return str(value)
        elif role == Qt.TextAlignmentRole:
            # Align numbers to the right, text to the left
            value = self._data_frame.iloc[index.row(), index.column()]
            if pd.api.types.is_numeric_dtype(type(value)) or (isinstance(value, str) and value.replace('.', '').replace('-', '').isdigit()):
                return int(Qt.AlignRight | Qt.AlignVCenter)
            return int(Qt.AlignLeft | Qt.AlignVCenter)
        elif role == Qt.BackgroundRole:
            # Flash updated cells with green background
            cell_key = (index.row(), index.column())
            if cell_key in self._updated_cells:
                return QColor('#90EE90')  # Light green flash
        return QVariant()

    def headerData(self, section, orientation, role=Qt.DisplayRole):
        if role == Qt.DisplayRole:
            if orientation == Qt.Horizontal:
                return str(self._data_frame.columns[section])
            else:
                return str(self._data_frame.index[section])
        return QVariant()

    def update_data(self, new_data_frame):
        """Update the data and trigger a refresh"""
        self.beginResetModel()
        self._data_frame = new_data_frame
        self.endResetModel()

    def flash_cell(self, row, col):
        """Flash a specific cell to indicate it was updated"""
        cell_key = (row, col)
        self._updated_cells.add(cell_key)
        # Emit dataChanged to trigger a redraw of this cell
        self.dataChanged.emit(self.index(row, col), self.index(row, col))
        # Start timer to clear the flash
        self._flash_timer.start(2000)  # Flash for 2 seconds

    def clear_flash(self):
        """Clear all flashed cells"""
        if self._updated_cells:
            self._updated_cells.clear()
            # Emit dataChanged for all cells to refresh the view
            self.dataChanged.emit(self.index(0, 0), self.index(self.rowCount()-1, self.columnCount()-1))

class ParquetUpdaterWindow(QMainWindow):
    def __init__(self, queue_monitor, auto_save_thread):
        super().__init__()
        self.queue_monitor = queue_monitor
        self.auto_save_thread = auto_save_thread
        self.parquet_dialog = None  # Store reference to the parquet dialog
        self.setWindowTitle("Kalshi Parquet Price Updater - Live Price Updates")
        self.setGeometry(100, 100, 1600, 1000)
        
        self.setStyleSheet("""
            QMainWindow{background-color:#ecf0f1}
            QFrame{background-color:#ffffff;border:1px solid #bdc3c7;border-radius:5px}
            QLabel{font-family:'Segoe UI',sans-serif;font-size:12px;font-weight:bold;color:#2c3e50}
            QPushButton{background-color:#2980b9;color:white;border:none;padding:8px 16px;border-radius:4px;font-weight:bold}
            QPushButton:hover{background-color:#3498db}
            QPushButton:pressed{background-color:#21618c}
            QTableView{background-color:#ffffff;alternate-background-color:#f8f9f9;gridline-color:#e0e0e0;border:1px solid #ddd;font-size:11px}
            QHeaderView::section{background-color:#bdc3c7;padding:5px;border:1px solid #cccccc;font-weight:bold}
            QTextEdit{background-color:#ffffff;border:1px solid #ddd;font-family:'Consolas',monospace;font-size:10px}
            QProgressBar{border:1px solid #ccc;border-radius:3px;text-align:center;background-color:#f0f0f0}
            QProgressBar::chunk{background-color:#007bff;border-radius:2px}
        """)
        
        self.central_widget = QWidget()
        self.setCentralWidget(self.central_widget)
        self.layout = QVBoxLayout(self.central_widget)
        
        self._init_ui()
        self.queue_monitor.new_trade_signal.connect(self.add_new_trade)
        self.auto_save_thread.save_complete_signal.connect(self.on_auto_save_complete)
        
        # Show parquet file content dialog after a short delay
        QTimer.singleShot(1000, self.show_parquet_content)
    
    def show_parquet_content(self):
        """Show the parquet file content dialog"""
        self.parquet_dialog = TickerListDialog(parquet_data, self)
        self.parquet_dialog.exec_()
    
    def _init_ui(self):
        # Control frame
        control_frame = QFrame()
        control_layout = QHBoxLayout(control_frame)
        
        self.status_label = QLabel("Initializing parquet price updater...")
        self.status_label.setStyleSheet("font-weight:normal;color:#34495e")
        control_layout.addWidget(self.status_label)
        
        control_layout.addStretch()
        
        # Show parquet content button
        self.show_parquet_btn = QPushButton("📋 Show Parquet File Content")
        self.show_parquet_btn.clicked.connect(self.show_parquet_content)
        control_layout.addWidget(self.show_parquet_btn)
        
        # Manual save button
        self.save_btn = QPushButton("💾 Save Updated Parquet")
        self.save_btn.clicked.connect(self.manual_save)
        self.save_btn.setEnabled(False)
        control_layout.addWidget(self.save_btn)
        
        # CSV file info
        csv_info = QLabel(f"Recording to: {csv_file_path}")
        csv_info.setStyleSheet("font-size:10px;color:#7f8c8d")
        control_layout.addWidget(csv_info)
        
        self.layout.addWidget(control_frame)
        
        # Stats frame
        stats_frame = QFrame()
        stats_layout = QHBoxLayout(stats_frame)
        
        self.total_tickers_label = QLabel(f"Total Parquet Tickers: {len(parquet_tickers)}")
        self.updated_tickers_label = QLabel("Updated Tickers: 0")
        self.total_trades_label = QLabel("Total Trades: 0")
        
        stats_layout.addWidget(self.total_tickers_label)
        stats_layout.addWidget(self.updated_tickers_label)
        stats_layout.addWidget(self.total_trades_label)
        stats_layout.addStretch()
        
        self.layout.addWidget(stats_frame)
        
        # Trade table
        self.table_view = QTableView()
        self.table_view.setAlternatingRowColors(True)
        self.table_view.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)
        self.table_view.setSortingEnabled(False)  # Keep manual order for latest trades at top
        
        self.model = ParquetUpdaterModel()
        self.table_view.setModel(self.model)
        
        self.layout.addWidget(self.table_view)
        
        # Log frame
        log_frame = QFrame()
        log_layout = QVBoxLayout(log_frame)
        
        log_label = QLabel("Recent Activity Log:")
        log_layout.addWidget(log_label)
        
        self.log_text = QTextEdit()
        self.log_text.setMaximumHeight(150)
        self.log_text.setReadOnly(True)
        log_layout.addWidget(self.log_text)
        
        self.layout.addWidget(log_frame)
    
    def add_new_trade(self, trade_data):
        """Add a new trade to the display"""
        self.model.add_trade(trade_data)
        
        # Update stats
        self.total_trades_label.setText(f"Total Trades: {len(self.model._data)}")
        self.updated_tickers_label.setText(f"Updated Tickers: {len(updated_tickers)}")
        
        # Enable save button if we have updates
        if len(updated_tickers) > 0:
            self.save_btn.setEnabled(True)
        
        # Update status
        status_text = f"Last trade: {trade_data['market_ticker']} at {trade_data['timestamp']}"
        if trade_data['parquet_updated']:
            status_text += " (Parquet Updated)"
        self.status_label.setText(status_text)
        
        # Add to log
        log_entry = f"[{trade_data['timestamp']}] {trade_data['market_ticker']} {trade_data['side']} @ {trade_data['price']}¢ (Qty: {trade_data['quantity']})"
        if trade_data['price_change'] != 0:
            log_entry += f" | Price Δ: {trade_data['price_change']:+.2f} ({trade_data['price_change_percent']:+.2f}%)"
        if trade_data['parquet_updated']:
            log_entry += " | ✅ Parquet Updated"
        
        self.log_text.append(log_entry)
        
        # Auto-scroll to keep latest visible
        self.table_view.scrollToTop()
        
        # Update the parquet dialog if it's open
        if self.parquet_dialog and trade_data['parquet_updated']:
            # Get the new price that was used
            ticker = trade_data['market_ticker']
            side = trade_data['side']
            
            # Find the new price from the updated parquet data
            mask = parquet_data['ticker'] == ticker
            if mask.any():
                row_idx = mask.idxmax()
                if side == 'YES':
                    new_price = parquet_data.loc[row_idx, 'yes_bid']
                else:  # NO
                    new_price = parquet_data.loc[row_idx, 'no_bid']
                
                # Update the dialog
                self.parquet_dialog.update_price(ticker, side, new_price)
    
    def manual_save(self):
        """Manually save the updated parquet data"""
        if parquet_data is not None and len(updated_tickers) > 0:
            output_file = save_updated_parquet()
            if output_file:
                QMessageBox.information(self, "Save Complete", f"Updated parquet data saved to:\n{output_file}")
                self.log_text.append(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] Manual save completed: {output_file}")
            else:
                QMessageBox.warning(self, "Save Failed", "Failed to save updated parquet data")
        else:
            QMessageBox.information(self, "No Updates", "No parquet data has been updated yet")
    
    def on_auto_save_complete(self, output_file):
        """Handle auto-save completion"""
        self.log_text.append(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] Auto-save completed: {output_file}")

def main():
    print("[DEBUG] Starting main()...")
    # Load parquet data first
    if not load_parquet_data():
        print("[ERROR] Failed to load parquet data. Exiting.")
        sys.exit(1)
    print("[DEBUG] Parquet data loaded.")
    # Initialize CSV file
    initialize_csv_file()
    print("[DEBUG] CSV file initialized.")
    # Start WebSocket thread
    websocket_thread = threading.Thread(target=run_websocket, daemon=True)
    websocket_thread.start()
    print("[DEBUG] WebSocket thread started.")
    # Start GUI
    app = QApplication(sys.argv)
    app.setFont(QFont("Segoe UI", 9))
    print("[DEBUG] QApplication created.")
    queue_monitor = QueueMonitorThread()
    auto_save_thread = AutoSaveThread(save_interval=300)  # Auto-save every 5 minutes
    updater = ParquetUpdaterWindow(queue_monitor, auto_save_thread)
    print("[DEBUG] ParquetUpdaterWindow created.")
    queue_monitor.start()
    print("[DEBUG] QueueMonitorThread started.")
    auto_save_thread.start()
    print("[DEBUG] AutoSaveThread started.")
    updater.show()
    print("[DEBUG] GUI shown.")
    # Handle application shutdown
    def cleanup():
        print("[DEBUG] Cleaning up threads and saving parquet data if needed...")
        auto_save_thread.stop()
        auto_save_thread.wait()
        if parquet_data is not None and len(updated_tickers) > 0:
            save_updated_parquet()
    app.aboutToQuit.connect(cleanup)
    print("[DEBUG] Entering app.exec_() event loop.")
    sys.exit(app.exec_())

if __name__ == "__main__":
    main()

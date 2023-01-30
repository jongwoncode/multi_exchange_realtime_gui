import multiprocessing as mp
import websocket
import requests
import json
import sys
import datetime
from PyQt5.QtWidgets import *
from PyQt5.QtCore import *
import finplot as fplt
import numpy as np
import pandas as pd
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

fplt.display_timezone = datetime.timezone.utc 
fplt.candle_bull_color = "#FF0000"
fplt.candle_bull_body_color = "#FF0000" 
fplt.candle_bear_color = "#0000FF"
fplt.candle_bear_body_color = "#0000FF"
np.set_printoptions(suppress=True)

class Binance(QThread) :
    def run(self, q_orderbook, q_chart) :
        def on_message(ws, message) :
            data = json.loads(message)
            if ('e' in data) :
                if data['e'] == 'depthUpdate' :
                    bids = np.array(data["b"], dtype=float)
                    asks = np.array(data["a"], dtype=float)
                    q_orderbook.put({'exchange' : 'binance', 'bids' : bids, 'asks' : asks})
                
                elif data['e'] == 'continuous_kline' :
                    q_chart.put({'exchange': 'binance', 'type' : 'update', 'timestamp': float(data['k']["t"]), 'open':float(data['k']['o']), 'high': float(data['k']['h']), "low" : float(data['k']['l']), 'close' : float(data['k']['c']), "volume": float(data['k']['n'])})


        def on_error(ws, error):
            print("Binance Websocket Error", error)

        def on_close(ws, close_status_code, close_msg):
            print("Binance Websocket is Closed", close_status_code, close_msg)

        def on_open(ws) :
            print("Binance Websocket is Open")
            sub_msg = {"method" : "SUBSCRIBE", "params" : ["btcusdt@depth10@100ms"], "id":1}
            ws.send(json.dumps(sub_msg))

            # request candle chart
            url = 'https://fapi.binance.com/fapi/v1/continuousKlines?pair=btcusdt&contractType=PERPETUAL&interval=1m&limit=1500'
            headers = {"accept": "application/json"}
            response = requests.get(url, headers=headers)
            df = pd.DataFrame(np.array(response.json())[:, :6], columns=['timestamp', 'Open', 'High', 'Low', 'Close', 'Volume'], dtype=float)
            q_chart.put({'exchange': 'binance', 'type' : 'snapshot', 'data' : df})

            sub_msg = {"method" : "SUBSCRIBE", "params" : ['btcusdt_perpetual@continuousKline_1m'], "id":1}
            ws.send(json.dumps(sub_msg))

        url = "wss://fstream.binance.com/ws"
        # websocket.enableTrace(True)
        ws = websocket.WebSocketApp(url = url, 
                                    on_open = on_open, 
                                    on_message = on_message, 
                                    on_close = on_close,
                                    on_error = on_error)
        ws.run_forever(reconnect=1, ping_interval=15) 



class Bitget(QThread) :
    def run(self, q_orderbook, q_chart) :
        def on_message(ws, message) :
            data = json.loads(message)
            if ('action' in data):
                if data['arg']['channel'] == 'books15' :
                    bids = np.array(data['data'][0]['bids'], dtype=float)
                    asks = np.array(data['data'][0]['asks'], dtype=float)
                    q_orderbook.put({'exchange' : 'bitget', 'bids' : bids, 'asks' : asks})
                    
                elif data['arg']['channel'] == 'candle1m' :
                    if data['action'] == 'snapshot' :
                        new_data = data['data']
                        df = pd.DataFrame(new_data, columns=['timestamp', 'Open', 'High', 'Low', 'Close', 'Volume'], dtype=float)
                        info = {'exchange': 'bitget', 'type' : 'snapshot', 'data' : df}
                        q_chart.put(info)


                    elif data['action'] == 'update' :
                        new_data = data['data']
                        info = {'exchange': 'bitget', 'type' : 'update', 'timestamp': float(new_data[0][0]), 'open':float(new_data[0][1]), 'high': float(new_data[0][2]), "low" : float(new_data[0][3]), 'close' : float(new_data[0][4]), "volume": float(new_data[0][5])}
                        q_chart.put(info)
                        
                    
        def on_error(ws, error):
            print("Bitget Websocket Error", error)

        def on_close(ws, close_status_code, close_msg):
            print("Bitget Websocket is Closed", close_status_code, close_msg)

        def on_open(ws) :
            print("Bitget Websocket is Open")
            sub_msg = {"op": "subscribe", "args": [{"instType":"mc","channel":"books15","instId":"BTCUSDT"},{"instType": "sp","channel": "candle1m","instId": "BTCUSDT"}]}
            ws.send(json.dumps(sub_msg))


        url = 'wss://ws.bitget.com/mix/v1/stream'
        # websocket.enableTrace(True)
        ws = websocket.WebSocketApp(url = url, 
                                    on_open = on_open, 
                                    on_message =  on_message, 
                                    on_close = on_close,
                                    on_error = on_error)
        ws.run_forever(reconnect=1, ping_interval=15)  # Set dispatcher to automatic reconnection, 5 second reconnect delay if connection closed unexpectedly

def bitget_producer(q_orderbook, q_chart) :
    bitget = Bitget()
    bitget.run(q_orderbook, q_chart)
 
def binance_producer(q_orderbook, q_chart) :   
    binance = Binance()
    binance.run(q_orderbook, q_chart)



class Consumer(QThread) :
    poped_orderbook = pyqtSignal(dict)
    poped_chart = pyqtSignal(dict)

    def __init__(self, q_orderbook, q_chart) :
        super().__init__()
        self.q_orderbook = q_orderbook
        self.q_chart = q_chart

    def run(self) :
        while True :
            if not self.q_chart.empty() :
                self.poped_chart.emit(self.q_chart.get())

            if not self.q_orderbook.empty() :
                self.poped_orderbook.emit(self.q_orderbook.get())


class MyWindow(QMainWindow):
    def __init__(self, q_orderbook, q_chart):
        super().__init__()
        t_width, t_height= 500, 620
        self.setGeometry(800, 200, int(t_width*2.2), int(t_height*1.6))
        self.setWindowTitle("Multi Exchange OrderBook")

        # thread for data consumer
        self.consumer = Consumer(q_orderbook, q_chart)
        self.consumer.poped_orderbook.connect(self.update_table)
        self.consumer.poped_chart.connect(self.update_chart)
        self.consumer.start()

        
        # ___QLabel___________________________________
        bitget_label = QLabel("Bitget", self)
        bitget_label.move(t_width*0+50, 10)

        binance_label = QLabel("Binance", self)
        binance_label.move(t_width*1+50, 10)


        # ___Table Wideget___________________________________
        self.tableWidget_bitget = QTableWidget(self)
        self.tableWidget_binance = QTableWidget(self)


        for i, tableWidget in enumerate([self.tableWidget_bitget, self.tableWidget_binance]) :
            tableWidget.move(t_width*i+30, int(t_height*0.5)+40)
            tableWidget.resize((t_width-20), t_height)
            tableWidget.setColumnCount(3)
            tableWidget.setRowCount(20)
            tableWidget.verticalHeader().setVisible(False)
            tableWidget.horizontalHeader().setVisible(False)
            tableWidget.setColumnWidth(0, int(tableWidget.width() * 0.4))
            tableWidget.setColumnWidth(1, int(tableWidget.width() * 0.2))
            tableWidget.setColumnWidth(2, int(tableWidget.width() * 0.4)) 


        self.bitget_graph = QGraphicsView(self)
        self.bitget_layout = QGridLayout(self.bitget_graph)
        self.bitget_graph.resize(t_width-20, 300)
        self.bitget_graph.move(t_width*0+30, 40)
        self.bitget_ax = fplt.create_plot(init_zoom_periods=100)    # pygtgraph.graphicsItems.PlotItem
        self.bitget_axs = [self.bitget_ax]                                 # finplot requres this property
        self.bitget_layout.addWidget(self.bitget_ax.vb.win, 0, 0)          # ax.vb     (finplot.FinViewBox)


        self.binance_graph = QGraphicsView(self)
        self.binance_layout = QGridLayout(self.binance_graph)
        self.binance_graph.resize(t_width-20, 300)
        self.binance_graph.move(t_width*1+30, 40)
        self.binance_ax = fplt.create_plot(init_zoom_periods=100)    # pygtgraph.graphicsItems.PlotItem
        self.binance_axs = [self.binance_ax]                                 # finplot requres this property
        self.binance_layout.addWidget(self.binance_ax.vb.win, 0, 0)          # ax.vb     (finplot.FinViewBox)


        # chart
        self.plot_bitget = None
        self.df_bitget = None
        self.plot_binance = None
        self.df_binance = None

    @pyqtSlot(dict)
    def update_chart(self, data) :
        # __update chart datafrane__
        if data['exchange'] == 'bitget' :
            # chart update
            if data['type'] == 'update' :
                timestamp = data['timestamp']/1000
                cur_min_timstamp = timestamp - timestamp%60
                cur_min_dt = datetime.datetime.fromtimestamp(cur_min_timstamp)
                # 분봉 업데이트시 df 추가
                if cur_min_dt > datetime.datetime.fromtimestamp(self.df_bitget.iloc[-1]['timestamp']/1000) :
                    new_chart = {'timestamp' : data['timestamp'], 'Open': data['open'], 'High': data['high'], 'Low': data['low'], 'Close': data['close'], 'Volume' : data['volume'] }
                    self.df_bitget = self.df_bitget.append(new_chart, ignore_index = True)

                    # 분봉의 수가 많아지면 처음 부분 제거
                    if len(self.df_bitget) > 5000 :
                        self.df_bitget = self.df_bitget.iloc[-4000:, :]
                else :
                    # update last candle
                    self.df_bitget.iloc[-1]['Close'] = data['close']
                    self.df_bitget.iloc[-1]['High'] = data['high']
                    self.df_bitget.iloc[-1]['Low'] = data['low']
                    self.df_bitget.iloc[-1]['Volume'] = data['volume']
                
                self.plot_bitget.update_data(self.df_bitget[['timestamp', 'Open', 'Close', 'High', 'Low']])

            # chart snapshot
            elif data['type'] == 'snapshot' :
                self.df_bitget = data['data']
                self.plot_bitget = fplt.candlestick_ochl(ax= self.bitget_ax,
                                                            datasrc = self.df_bitget[['timestamp', 'Open', 'Close', 'High', 'Low']])
                fplt.show(qt_exec=False)



        if data['exchange'] == 'binance' :
            # chart update
            if data['type'] == 'update' :
                timestamp = data['timestamp']/1000
                cur_min_timstamp = timestamp - timestamp%60
                cur_min_dt = datetime.datetime.fromtimestamp(cur_min_timstamp)
                # 분봉 업데이트시 df 추가
                if cur_min_dt > datetime.datetime.fromtimestamp(self.df_binance.iloc[-1]['timestamp']/1000) :
                    new_chart = {'timestamp' : data['timestamp'], 'Open': data['open'], 'High': data['high'], 'Low': data['low'], 'Close': data['close'], 'Volume' : data['volume'] }
                    self.df_binance = self.df_binance.append(new_chart, ignore_index = True)

                    # 분봉의 수가 많아지면 처음 부분 제거
                    if len(self.df_binance) > 5000 :
                        self.df_binance = self.df_binance.iloc[-4000:, :]
                else :
                    # update last candle
                    self.df_binance.iloc[-1]['Close'] = data['close']
                    self.df_binance.iloc[-1]['High'] = data['high']
                    self.df_binance.iloc[-1]['Low'] = data['low']
                    self.df_binance.iloc[-1]['Volume'] = data['volume']
                
                self.plot_binance.update_data(self.df_binance[['timestamp', 'Open', 'Close', 'High', 'Low']])

            # chart snapshot
            elif data['type'] == 'snapshot' :
                self.df_binance = data['data']
                self.plot_binance = fplt.candlestick_ochl(ax= self.binance_ax,
                                                                datasrc = self.df_binance[['timestamp', 'Open', 'Close', 'High', 'Low']])
                fplt.show(qt_exec=False)



        

    @pyqtSlot(dict)
    def update_table(self, data) : 
        exchange = data.get('exchange')
        tableWidget = None
        if exchange == 'bitget' :
            tableWidget = self.tableWidget_bitget
        if exchange == 'binance' :
            tableWidget = self.tableWidget_binance

        bids, asks = data['bids'], data['asks'] 
        #___ Price_______________
        for i in range(10) :
            # __bid________________
            bid_item = QTableWidgetItem(format(bids[i][0], ","))
            bid_item.setTextAlignment(int(Qt.AlignRight|Qt.AlignVCenter))
            tableWidget.setItem(10+i, 1, bid_item)

            # __ask________________
            ask_item = QTableWidgetItem(format(asks[i][0], ","))
            ask_item.setTextAlignment(int(Qt.AlignRight|Qt.AlignVCenter))
            tableWidget.setItem(9-i, 1, ask_item)
        
        #___Volume_______________
        for i in range(10) :
            max_volume = int(max(bids.max(axis=0)[1], asks.max(axis=0)[1]))+1
            bid_volume =  bids[i][1]
            bid_widget = QWidget()
            bid_layout = QVBoxLayout(bid_widget)
            bid_pbar = QProgressBar()
            bid_pbar.setFixedHeight(20)
            bid_pbar.setAlignment(Qt.AlignLeft|Qt.AlignVCenter)
            bid_pbar.setStyleSheet("""QProgressBar::Chunk {background-color : rgba(255, 0, 0, 20%);border : 1}""")
            bid_layout.addWidget(bid_pbar)
            bid_layout.setAlignment(Qt.AlignVCenter)
            bid_layout.setContentsMargins(0, 0, 0, 0)
            bid_widget.setLayout(bid_layout)
            tableWidget.setCellWidget(10+i, 2, bid_widget)
            # set data 
            bid_pbar.setRange(0, max_volume)
            bid_pbar.setFormat(str(bid_volume))
            bid_pbar.setValue(int(bid_volume))

            # __ ask volume ________________
            ask_volume =  asks[i][1]
            ask_widget = QWidget()
            ask_layout = QVBoxLayout(ask_widget)
            ask_pbar = QProgressBar()
            ask_pbar.setFixedHeight(20)
            ask_pbar.setInvertedAppearance(True)  
            ask_pbar.setAlignment(Qt.AlignRight|Qt.AlignVCenter)
            ask_pbar.setStyleSheet("""QProgressBar::Chunk {background-color : rgba(0, 0, 255, 20%);border : 1}""")
            ask_layout.addWidget(ask_pbar)
            ask_layout.setAlignment(Qt.AlignVCenter)
            ask_layout.setContentsMargins(0, 0, 0, 0)
            ask_widget.setLayout(ask_layout)
            tableWidget.setCellWidget(9-i, 0, ask_widget)
            # set data 
            ask_pbar.setRange(0, max_volume)
            ask_pbar.setFormat(str(ask_volume))
            ask_pbar.setValue(int(ask_volume))



if __name__ == "__main__":
    q_orderbook = mp.Queue()
    q_chart = mp.Queue()
    bitget_p = mp.Process(name="Producer",  target=bitget_producer, args=(q_orderbook, q_chart), daemon=True)
    bitget_p.start()

    binance_p = mp.Process(name="Producer",  target=binance_producer, args=(q_orderbook, q_chart), daemon=True)
    binance_p.start()

    # Main process
    app = QApplication(sys.argv)
    mywindow = MyWindow(q_orderbook, q_chart)
    mywindow.show()
    app.exec_()
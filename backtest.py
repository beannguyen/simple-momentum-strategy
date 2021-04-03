import backtrader as bt
import pandas as pd
import matplotlib.pyplot as plt


class MomStrategy(bt.Strategy):
    # use lookback period as 12 Business Month
    params = (
        ('period', 12 * 20),
        ('top_quintile', 10),
    )

    def log(self, txt, dt=None):
        ''' Logging function for this strategy'''
        dt = dt or self.datas[0].datetime.date(0)
        print('%s, %s' % (dt.isoformat(), txt))

    def __init__(self):
        self.month_year = []
        self.vnindex = self.datas[0]
        self.stocks = self.datas[1:]
        self.inds = {}

        for d in self.stocks:
            self.inds[d] = {}
            self.inds[d]["roc"] = bt.indicators.Momentum(d.close, period=self.p.period)

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            # Buy/Sell order submitted/accepted to/by broker - Nothing to do
            return

        # Check if an order has been completed
        # Attention: broker could reject order if not enough cash
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(
                    'BUY EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f' %
                    (order.executed.price,
                     order.executed.value,
                     order.executed.comm))

                self.buyprice = order.executed.price
                self.buycomm = order.executed.comm
            else:  # Sell
                self.log('SELL EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f' %
                         (order.executed.price,
                          order.executed.value,
                          order.executed.comm))

            self.bar_executed = len(self)

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('Order Canceled/Margin/Rejected')

        self.order = None

    def notify_trade(self, trade):
        if not trade.isclosed:
            return

        self.log('OPERATION PROFIT, GROSS %.2f, NET %.2f' %
                 (trade.pnl, trade.pnlcomm))

    def prenext(self):
        # call next() even when data is not available for all tickers
        self.next()

    def next(self):
        current_date = self.datas[0].datetime.date(0)
        current_month = f'{current_date.year}{current_date.month}'
        if current_month not in self.month_year:
            self.rebalance()
            self.month_year.append(current_month)

    @property
    def open_positions(self):
        return list(filter(lambda d: self.getposition(d).size > 0, self.broker.positions))

    def rebalance(self):
        self.rankings = list(filter(lambda d: len(d) > 100, self.stocks))
        self.rankings.sort(key=lambda d: self.inds[d]["roc"][0], reverse=True)

        # close all positions
        for i, d in enumerate(self.open_positions):
            self.close(d)

        portfolio_value = self.broker.get_value()
        weight = 1 / self.p.top_quintile
        for i, d in enumerate(self.rankings[:int(self.p.top_quintile)]):
            size = int(portfolio_value * weight / d.close[0])
            self.buy(d, size=size)


if __name__ == '__main__':
    start_date = '2010-01-01'
    tickers = pd.read_csv('./data/tickers.csv')['tickers'].tolist()
    print(f'Number of stocks: {len(tickers)}')
    cerebro = bt.Cerebro()
    cerebro.broker.setcash(100000.0)

    vnindex_df = pd.read_csv(f"data/VNINDEX.csv",
                             parse_dates=True,
                             index_col=['date'])

    vnindex_df.dropna(inplace=True)
    date_range = (vnindex_df.index >= start_date)
    vnindex = bt.feeds.PandasData(dataname=vnindex_df[date_range], plot=False)
    cerebro.adddata(vnindex, name='vnindex')  # add VNIndex
    cerebro.addanalyzer(bt.analyzers.TimeReturn, data=vnindex, _name='benchmark')
    cerebro.addanalyzer(bt.analyzers.TimeReturn)

    for ticker in tickers:
        df = pd.read_csv(f"data/{ticker}.csv",
                         parse_dates=True,
                         index_col=['timestamp'])

        df.dropna(inplace=True)
        if len(df) > 12 * 20:  # data must be long enough to compute 12 months
            date_range = (df.index >= start_date)
            cerebro.adddata(bt.feeds.PandasData(dataname=df[date_range], plot=False), name=ticker)

    print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())

    cerebro.addobserver(bt.observers.Value)
    cerebro.addanalyzer(bt.analyzers.PyFolio)
    cerebro.addstrategy(MomStrategy)

    results = cerebro.run()

    print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())

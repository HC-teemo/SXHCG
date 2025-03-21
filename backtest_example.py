import hikyuu as hku
import pandas as pd

# 初始化 Hikyuu 数据库
# hku.init()

# 计算不同周期的 RPS
def calculate_rps(stock_list, start_date, end_date, period):
    rps_dict = {}
    sm = hku.StockManager.instance()
    benchmark = sm.get_stock("sh000001")  # 上证指数作为基准
    benchmark_kdata = benchmark.get_kdata(hku.KQueryByDate(start_date, end_date))
    benchmark_start_price = benchmark_kdata[0].close
    benchmark_end_price = benchmark_kdata[-1].close
    benchmark_return = (benchmark_end_price - benchmark_start_price) / benchmark_start_price

    for stock in stock_list:
        stk = sm.get_stock(stock)
        # 修改方法名
        kdata = stk.get_kdata(hku.KQueryByDate(start_date, end_date))
        if len(kdata) == 0:
            continue
        start_price = kdata[0].close
        end_price = kdata[-1].close
        stock_return = (end_price - start_price) / start_price
        rps = (stock_return - benchmark_return) / benchmark_return
        rps_dict[stock] = rps

    return rps_dict

# 创建 RPS 综合排名 Top5 策略系统
def create_rps_top5_strategy(start_date, end_date):
    sm = hku.StockManager.instance()
    all_stocks = sm.get_stock_list()
    stock_list = [stock.market_code for stock in all_stocks if stock.type == 1]  # 只考虑股票

    # 计算不同周期的 RPS
    rps50_dict = calculate_rps(stock_list, start_date, end_date, 50)
    rps120_dict = calculate_rps(stock_list, start_date, end_date, 120)
    rps250_dict = calculate_rps(stock_list, start_date, end_date, 250)

    # 合并 RPS 数据
    rps_combined = {}
    for stock in stock_list:
        rps50 = rps50_dict.get(stock, 0)
        rps120 = rps120_dict.get(stock, 0)
        rps250 = rps250_dict.get(stock, 0)
        rps_combined[stock] = rps50 + rps120 + rps250

    rps_df = pd.DataFrame.from_dict(rps_combined, orient='index', columns=['RPS_Total'])
    rps_df = rps_df.sort_values(by='RPS_Total', ascending=False)
    top5_stocks = rps_df.head(5).index.tolist()

    sys_list = []
    for stock_code in top5_stocks:
        stock = sm.get_stock(stock_code)
        # 修改方法名
        kdata = stock.get_kdata(hku.KQuery(-200))  # 获取最近 200 个交易日的 K 线数据

        # 这里简单使用买入持有策略
        sig = hku.SG_FixedSignal()
        mm = hku.MM_FixedCount(100)
        sys = hku.SysSimple()
        sys.setSig(sig)
        sys.setMM(mm)
        sys.setTO(kdata)
        sys_list.append(sys)

    return sys_list

# 运行回测
def run_backtest(start_date, end_date):
    sys_list = create_rps_top5_strategy(start_date, end_date)

    # 创建一个初始资金为 100000 的账户
    init_cash = 100000
    account = hku.Account(init_cash)

    total_final_cash = 0
    for sys in sys_list:
        # 为每个策略系统分配相同的资金
        sub_account = hku.Account(init_cash / len(sys_list))
        hku.backtest(sub_account, sys)
        total_final_cash += sub_account.currentCash

    # 输出回测结果
    print("初始资金:", init_cash)
    print("最终资金:", total_final_cash)
    print("盈利情况:", total_final_cash - init_cash)

if __name__ == "__main__":
    start_date = hku.Datetime(2024, 1, 1)
    end_date = hku.Datetime(2024, 12, 31)
    run_backtest(start_date, end_date)
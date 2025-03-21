import pandas as pd
import numpy as np
import h5py
from datetime import datetime, timedelta
from tqdm import tqdm
import os
from hikyuu.interactive import *

def calculate_daily_rps(start_date=None, end_date=None, periods=[10, 20, 50, 120, 250], output_file='daily_rps.h5'):
    """
    计算每个股票每天的RPS值并存储到HDF5文件
    
    参数:
        start_date: 开始日期，格式为'YYYY-MM-DD'，默认为一年前
        end_date: 结束日期，格式为'YYYY-MM-DD'，默认为今天
        periods: RPS计算周期列表，默认为[10, 20, 50, 120, 250]
        output_file: 输出的HDF5文件名
    """
    # 初始化日期
    # if end_date is None:
    #     end_date = datetime.now().strftime('%Y-%m-%d')
    # if start_date is None:
    #     # 默认从end_date往前推一年，确保有足够的数据计算RPS250
    #     start_date_dt = datetime.strptime(end_date, '%Y-%m-%d') - timedelta(days=365*2)
    #     start_date = start_date_dt.strftime('%Y-%m-%d')
    
    print(f"计算从 {start_date} 到 {end_date} 的每日RPS...")
    
    # 获取所有股票
    all_stocks = blocka
    print(f"共获取 {len(all_stocks)} 只股票")
    
    # 获取交易日历
    base_stock = get_stock('sh000001')  # 使用上证指数作为基准获取交易日历
    base_kdata = base_stock.get_kdata(Query(start_date, end_date))
    trading_dates = [k.datetime  for k in base_kdata]
    
    print(f"交易日期范围: {trading_dates[0]} 到 {trading_dates[-1]}，共 {len(trading_dates)} 个交易日")
    
    # 获取每只股票的上市日期
    stock_list_date = {}
    for stock in all_stocks:
        try:
            # 获取第一条K线数据的日期作为上市日期
            first_k = stock.get_kdata(Query(0, 1))
            if len(first_k) > 0:
                list_date = first_k[0].datetime
                stock_list_date[stock.code] = list_date
        except Exception as e:
            print(f"获取股票 {stock.code} 上市日期时出错: {e}")
    
    print(f"获取到 {len(stock_list_date)} 只股票的上市日期")
    
    # 创建HDF5文件
    with h5py.File(output_file, 'w') as f:
        # 为每个交易日创建一个组
        for date in tqdm(trading_dates, desc="处理交易日"):
            date_group = f.create_group(str(date.ymd))
            
            # 计算该日期一年前的日期
            one_year_ago = date - TimeDelta(days=365)
            
            # 收集该日期上市满一年的股票
            valid_stocks = []
            for stock in all_stocks:
                if stock.code in stock_list_date and stock_list_date[stock.code] <= one_year_ago:
                    valid_stocks.append(stock)
            
            # 如果有效股票数量太少，跳过
            if len(valid_stocks) < 10:
                print(f"日期 {date} 的有效股票数量过少: {len(valid_stocks)}，跳过")
                continue
                
            # 计算每个周期的RPS
            for period in periods:

                # 收集所有股票在该周期的涨幅
                stock_returns = []
                
                for stock in valid_stocks:
                    try:
                        # 获取该日期及其period天前的K线数据
                        end_query = Query(date)
                        kdata_end = stock.get_kdata(end_query)
                        
                        if len(kdata_end) == 0:
                            continue  # 该日期没有数据，可能停牌
                        
                        # 获取period天前的收盘价
                        # start_date_dt = datetime.strptime(date, '%Y-%m-%d') - timedelta(days=period*2)  # 多取一些天以确保有数据
                        # start_date_str = start_date_dt.strftime('%Y-%m-%d')
                        start_ops = kdata_end.start_pos - period
                        
                        kdata_start = stock.get_kdata(Query(start_ops))
                        # if len(kdata_all) <= period:
                        #     continue  # 数据不足，跳过
                        
                        # 计算涨幅
                        current_close = kdata_end[0].close
                        past_close = kdata_start[0].close
                        
                        if past_close <= 0:
                            continue  # 避免除以零
                        
                        return_rate = (current_close / past_close - 1) * 100
                        stock_returns.append((stock.code, return_rate))
                    
                    except Exception as e:
                        print(f"计算股票 {stock.code} 在 {date} 的RPS{period} 时出错: {e}")
                        continue
                
                # 如果收集到的数据太少，跳过
                if len(stock_returns) < 10:
                    print(f"日期 {date} 的RPS{period} 有效数据过少: {len(stock_returns)}，跳过")
                    continue
                
                # 按涨幅排序
                stock_returns.sort(key=lambda x: x[1], reverse=True)
                
                # 计算RPS
                total_stocks = len(stock_returns)
                rps_values = {}
                
                for i, (code, _) in enumerate(stock_returns):
                    # RPS = (总数 - 排名) / 总数 * 100
                    rps = (total_stocks - i) / total_stocks * 100
                    rps_values[code] = rps
                

                # 创建该周期的数据集
                rps_dataset = date_group.create_dataset(f'RPS{period}', (len(rps_values), 2), 
                                                      dtype=h5py.special_dtype(vlen=str))
                
                # 存储数据
                for i, (code, rps) in enumerate(rps_values.items()):
                    rps_dataset[i] = [code, str(rps)]
    
    print(f"RPS数据已保存到 {output_file}")

def load_rps_data(h5_file, date, period):
    """
    从HDF5文件加载特定日期和周期的RPS数据
    
    参数:
        h5_file: HDF5文件路径
        date: 日期字符串，格式为'YYYY-MM-DD'
        period: RPS周期，如10, 20, 50, 120, 250
    
    返回:
        包含股票代码和RPS值的字典
    """
    rps_dict = {}
    
    with h5py.File(h5_file, 'r') as f:
        if date in f:
            date_group = f[date]
            rps_key = f'RPS{period}'
            
            if rps_key in date_group:
                rps_dataset = date_group[rps_key]
                for code, rps in rps_dataset:
                    rps_dict[code] = float(rps)
    
    return rps_dict

def get_top_rps_stocks(h5_file, date, periods=[10, 20, 50, 120, 250], top_n=5, weighted=True):
    """
    获取指定日期RPS总和排名前N的股票
    
    参数:
        h5_file: HDF5文件路径
        date: 日期字符串，格式为'YYYY-MM-DD'
        periods: 要考虑的RPS周期列表
        top_n: 返回前N名股票
        weighted: 是否使用加权和，如果为True，则较短周期RPS权重更高
    
    返回:
        排名前N的股票列表，每个元素为(代码, RPS总和)
    """
    # 加载所有周期的RPS数据
    all_rps_data = {}
    
    for period in periods:
        rps_dict = load_rps_data(h5_file, date, period)
        for code, rps in rps_dict.items():
            if code not in all_rps_data:
                all_rps_data[code] = {}
            all_rps_data[code][f'RPS{period}'] = rps
    
    # 计算RPS总和
    rps_sum = []
    
    for code, rps_values in all_rps_data.items():
        # 检查是否所有周期的RPS都有
        if len(rps_values) == len(periods):
            if weighted:
                # 加权和，较短周期权重更高
                total = 0
                weight_sum = 0
                for period in periods:
                    weight = 1 / period  # 权重与周期成反比
                    total += rps_values[f'RPS{period}'] * weight
                    weight_sum += weight
                rps_sum.append((code, total / weight_sum))
            else:
                # 简单求和
                total = sum(rps_values.values())
                rps_sum.append((code, total))
    
    # 排序并返回前N名
    rps_sum.sort(key=lambda x: x[1], reverse=True)
    return rps_sum[:top_n]

# 使用示例
if __name__ == "__main__":
    # 初始化Hikyuu
    # import hikyuu as hku
    # hku.init()
    
    # 定义输出文件
    output_file = 'daily_rps.h5'
    
    # 计算每日RPS数据
    # 注意：这个过程可能会很耗时，取决于股票数量和日期范围
    start_date = Datetime('2024-01-01')
    end_date = Datetime('2025-03-21')
    
    # 如果文件不存在，则计算并保存RPS数据
    if not os.path.exists(output_file):
        calculate_daily_rps(start_date=start_date, end_date=end_date, output_file=output_file)
    
    # 查询特定日期的RPS排名前5的股票
    # test_date = '20250320'  # 确保这是交易日
    # top_stocks = get_top_rps_stocks(output_file, test_date, top_n=5)
    
    # print(f"\n{test_date} RPS总和排名前5的股票:")
    # for code, rps_sum in top_stocks:
    #     stock = get_stock(code)
    #     print(f"{code} {stock.name}: RPS总和 = {rps_sum:.2f}")
    
    # # 查看特定股票在特定日期的各周期RPS值
    # if top_stocks:
    #     top_code = top_stocks[0][0]
    #     print(f"\n股票 {top_code} 在 {test_date} 的各周期RPS值:")
    #     for period in [10, 20, 50, 120, 250]:
    #         rps_dict = load_rps_data(output_file, test_date, period)
    #         if top_code in rps_dict:
    #             print(f"RPS{period} = {rps_dict[top_code]:.2f}")



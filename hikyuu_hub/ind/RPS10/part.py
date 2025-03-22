
#!/usr/bin/env python
# -*- coding:utf-8 -*-

from hikyuu import *
import pandas as pd
import numpy as np
from datetime import datetime
import h5py
from tqdm import tqdm

from hikyuu.indicator import Indicator, IndicatorImp

author = "lsder"
version = "20250322"
def read_rps_to_dataframe(h5_file, rps_period=10):
    
    print(f"读取 {h5_file} 中的RPS{rps_period}数据...")
    
    # 用于存储所有日期的数据
    all_dates = []
    all_stocks = set()
    date_rps_dict = {}
    
    # 打开HDF5文件
    with h5py.File(h5_file, 'r') as f:
        # 获取所有日期
        dates = list(f.keys())
        dates.sort()  # 确保日期有序
        
        # 遍历每个日期
        for date in tqdm(dates, desc=f"读取RPS{rps_period}数据"):
            date_group = f[date]
            rps_key = f'RPS{rps_period}'
            
            # 检查该日期是否有对应的RPS数据
            if rps_key in date_group:
                rps_dataset = date_group[rps_key]
                
                # 提取该日期的RPS数据
                date_data = {}
                for code, rps in rps_dataset:
                    code_str = code.decode('utf-8') if isinstance(code, bytes) else code
                    rps_float = float(rps.decode('utf-8')) if isinstance(rps, bytes) else float(rps)
                    date_data[code_str] = rps_float
                    all_stocks.add(code_str)
                
                # 保存该日期的数据
                all_dates.append(date)
                date_rps_dict[date] = date_data
    
    print(f"共读取 {len(all_dates)} 个交易日的RPS{rps_period}数据")
    print(f"共涉及 {len(all_stocks)} 只股票")
    
    # 将所有股票代码转换为有序列表
    stock_codes = sorted(list(all_stocks))
    
    # 创建DataFrame
    df_data = []
    
    for date in all_dates:
        row = [date]  # 第一列为日期
        date_data = date_rps_dict[date]
        
        # 添加每只股票的RPS值，如果没有则为NaN
        for code in stock_codes:
            row.append(date_data.get(code, np.nan))
        
        df_data.append(row)
    
    # 创建列名
    columns = ['date'] + stock_codes
    
    # 构建DataFrame
    df = pd.DataFrame(df_data, columns=columns)
    
    # 将日期列转换为datetime类型
    df['date'] = pd.to_datetime(df['date'])
    
    # 设置日期列为索引
    # df = df.set_index('date')
    
    return df
def part():
    """doc"""
    file_directory = os.path.abspath('.')
    h5_file = file_directory+'/daily_rps.h5'
    print(h5_file)
    rps_period = 10
    df_rps10 = read_rps_to_dataframe(h5_file, rps_period)
    ret = Indicator()
    ret.name = "RPS10"
    
    code = CLOSE().get_context().get_stock().code
    code = code if code!='' else "000001"
    print(code)
    ret= df_to_ind(df_rps10, str(code),  'date')
    ret.name = "RPS10"
    return ret
    

if __name__ == "__main__":
    # 执行 testall 命令时，会多传入一个参数，防止测试时间过长
    # 比如如果在测试代码中执行了绘图操作，可以打开下面的注释代码
    # 此时执行 testall 命令时，将直接返回
    if len(sys.argv) > 1:
        ind = part()
        print(ind)
        exit(0)

    import sys
    if sys.platform == 'win32':
        import os
        os.system('chcp 65001')

    # 仅加载测试需要的数据，请根据需要修改
    options = {
        'stock_list': ['sz000001'],
        'ktype_list': ['day'],
        'preload_num': {'day_max': 100000},
        'load_history_finance': False,
        'load_weight': False,
        'start_spot': False,
        'spot_worker_num': 1,
    }
    load_hikyuu(**options)
    
    stks = tuple([sm[code] for code in options['stock_list']])
    
    # 请在下方编写测试代码
    ind = part()
    print(ind)
    
    # 显示图形
    import matplotlib.pylab as plt
    plt.show()    

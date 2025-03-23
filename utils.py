import pandas as pd
from tqdm import tqdm
import numpy as np
import h5py
def read_rps_file(h5_file, rps_period=10):
    
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
    df['datetime'] = pd.to_datetime(df['date'])
    
    # 设置日期列为索引
    # df = df.set_index('datetime')
    
    return df


def read_guchi(file_path):
    stock_list = []
    try:
        # 尝试打开文件，通常是 GB2312 编码
        with open(file_path, 'r', encoding='gb2312', errors='ignore') as f:
            lines = f.readlines()
        for line in lines:
            # 去除空白字符
            line = line.strip()
            
            # 跳过空行
            if not line:
                continue

            code = line
            if code:
                # sz
                if code.startswith('0'):
                    stock_list.append("sz"+code[1:])
                elif code.startswith('1'):
                    stock_list.append("sh"+code[1:])
    except Exception as e:
        print(f"导入 EBK 文件时出错: {e}")
    
    return stock_list     
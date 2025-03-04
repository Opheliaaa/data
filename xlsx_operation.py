import pandas as pd
import requests
import re
import os
from datetime import datetime
from urllib.parse import unquote
from concurrent.futures import ThreadPoolExecutor

if not os.path.exists('sgx'):
    os.mkdir('sgx')
if not os.path.exists('output'):
    os.mkdir('output')



headers = {
    'accept': '*/*',
    'accept-language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7',
    'cache-control': 'no-cache',
    'origin': 'https://www.sgx.com',
    'pragma': 'no-cache',
    'priority': 'u=1, i',
    'referer': 'https://www.sgx.com/',
    'sec-ch-ua': '"Not(A:Brand";v="99", "Google Chrome";v="133", "Chromium";v="133"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-site',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36',
}

# 定义一个函数，将日期字符串转换为周日期格式
def convert_week_date(date_str):
    # 将日期字符串中的"Week of "替换为空
    date_part = date_str.replace('Week of ', '')
    # 将日期字符串中的"to \d+"替换为空
    date_part = re.sub('to \d+', r'', date_part)
    # 尝试将日期字符串转换为日期对象，格式为"%d %B %Y"
    try:
        date = datetime.strptime(date_part, '%d %B %Y')
    # 如果转换失败，则尝试将日期字符串转换为日期对象，格式为"%d %b %Y"
    except ValueError:
        try:
            date = datetime.strptime(date_part, '%d %b %Y')
        # 如果转换失败，则打印错误信息，并返回空字符串
        except ValueError as e:
            print(f"Error parsing date: {date_part}")
            return ''
    # 将日期对象的月份、日期和年份分别转换为字符串
    month = str(date.month)
    day = str(date.day)
    year = date.strftime('%y')
    # 返回格式为"月/日/年"的日期字符串
    return f"{month}/{day}/{year}"



def parse_weekly():
    # 打印函数名
    print('parse_weekly')
    # 定义两个空列表，用于存储处理后的数据
    df1s = []
    df2s = []
    # 遍历文件夹sgx中的所有文件
    for root, dirs, files in os.walk('sgx'):
        for file in files:
            # 判断文件是否以.xlsx结尾
            if file.endswith('.xlsx'):
                # 尝试从文件名中提取日期
                try:
                    sdate = re.findall(r'\((.*?)\)', file)[0]
                except:
                    # 如果提取失败，则从文件名中提取日期
                    sdate = re.findall(r'(Week of .*?)\.xlsx', file)[0]
                # 获取文件路径
                file_path = os.path.join(root, file)
                # 尝试读取文件中的'Weekly Top 10'表
                try:
                    df = pd.read_excel(file_path, sheet_name='Weekly Top 10')
                except:
                    # 如果读取失败，则打印错误信息并跳过该文件
                    print(f"{file_path} has no sheet named 'Weekly Top 10'")
                    continue
                # 删除前三行和从第27行开始到最后一行的数据
                df.drop(index=range(3), inplace=True)
                df.drop(index=range(27, df.index[-1]+1), inplace=True)
                # 获取空行的起始和结束索引
                sindex = min(list(df[df.isnull().all(axis=1)].index))
                mindex = max(list(df[df.isnull().all(axis=1)].index))
                # 根据空行的索引，将数据分为两部分
                df1 = df.loc[:sindex-1].reset_index(drop=True)
                df2 = df.loc[mindex+1:].reset_index(drop=True)
                # 定义表头
                headers1 = ['Top 10 Institution Net Buy Name', 'Stock Code', 'Net Buy', 'Top 10 Institution Net Sell Name', 'Stock Code', 'Net Sell']
                headers2 = ['Top 10 Retail Net Buy Name', 'Stock Code', 'Net Buy', 'Top 10 Retail Net Sell Name', 'Stock Code', 'Net Sell']
                # 删除多余列
                df1 = df1[1:]
                df2 = df2[1:]
                df1.drop(columns=df1.columns[6:], inplace=True)
                df2.drop(columns=df2.columns[6:], inplace=True)
                # 设置表头
                df1.columns = headers1
                df2.columns = headers2
                # 添加日期列
                df1['Date'] = convert_week_date(sdate)
                df2['Date'] = convert_week_date(sdate)
                # 将处理后的数据添加到列表中
                df1s.append(df1)
                df2s.append(df2)
    # 将列表中的数据合并为一个DataFrame
    ndf1 = pd.concat(df1s, ignore_index=True, axis=0)
    ndf2 = pd.concat(df2s, ignore_index=True, axis=0)
    # 将合并后的数据保存为Excel文件
    ndf1.to_excel('output\\df1.xlsx', index=False)
    ndf2.to_excel('output\\df2.xlsx', index=False)
    # 返回合并后的数据
    return ndf1, ndf2


def parse_sti():
    # 打印函数名
    print('parse_sti')
    # 创建一个空列表，用于存储读取的DataFrame
    df3s = []
    # 遍历sgx文件夹下的所有文件
    for root, dirs, files in os.walk('sgx'):
        for file in files:
            # 判断文件是否以.xlsx结尾
            if file.endswith('.xlsx'):
                # 尝试从文件名中提取日期
                try:
                    sdate = re.findall(r'\((.*?)\)', file)[0]
                except:
                    # 如果提取失败，则从文件名中提取日期
                    sdate = re.findall(r'(Week of .*?)\.xlsx', file)[0]
                # 获取文件路径
                file_path = os.path.join(root, file)
                # 尝试读取Excel文件中的'STI Constituents'表
                try:
                    df = pd.read_excel(file_path, sheet_name='STI Constituents')
                except:
                    # 如果读取失败，则打印错误信息并跳过该文件
                    print(f"{file_path} has no sheet named 'STI Constituents'")
                    continue
                # 找到DataFrame中第一个全为空的行的索引
                kindex = min(list(df[df.isnull().all(axis=1)].index))
                # 截取DataFrame中从第一行到kindex-1行的数据
                df3 = df.loc[:kindex-1].reset_index(drop=True)
                # 删除DataFrame中第5列之后的所有列
                df3.drop(columns=df3.columns[4:], inplace=True)
                # 删除DataFrame中最后一行
                df3 = df3[:-1]
                # 重命名DataFrame的列名
                df3.columns = ['STI Constituents', 'Stock Coce', 'Institution Net Buy (+) / Net Sell (-) (S$M)', 'Retail Net Buy (+) / Net Sell (-) (S$M)']
                # 将日期转换为标准格式
                df3['Date'] = convert_week_date(sdate)
                # 将处理后的DataFrame添加到列表中
                df3s.append(df3)
                # 将处理后的DataFrame保存为Excel文件
                # df3.to_excel('df3.xlsx', index=False)
    # 将列表中的所有DataFrame合并为一个DataFrame
    ndf3 = pd.concat(df3s, ignore_index=True, axis=0)
    # 将合并后的DataFrame保存为Excel文件
    ndf3.to_excel('output\\df3.xlsx', index=False)
    # 返回合并后的DataFrame
    return ndf3

def parse_100():
    # 打印函数名
    print('parse_100')
    # 创建一个空列表，用于存储读取的DataFrame
    df4s = []
    # 遍历文件夹sgx中的所有文件
    for root, dirs, files in os.walk('sgx'):
        for file in files:
            # 判断文件是否以.xlsx结尾
            if file.endswith('.xlsx'):
                # 尝试从文件名中提取日期
                try:
                    sdate = re.findall(r'\((.*?)\)', file)[0]
                except:
                    # 如果提取失败，则从文件名中提取日期
                    sdate = re.findall(r'(Week of .*?)\.xlsx', file)[0]
                # 获取文件路径
                file_path = os.path.join(root, file)
                # 尝试读取Excel文件中的'100 Most Traded Stocks'表
                try:
                    df = pd.read_excel(file_path, sheet_name='100 Most Traded Stocks')
                except:
                    # 如果读取失败，则打印错误信息并跳过该文件
                    print(f"{file_path} has no sheet named '100 Most Traded Stocks'")
                    continue
                # 找到DataFrame中第一个全为空的行的索引
                kindex = min(list(df[df.isnull().all(axis=1)].index))
                # 截取DataFrame中前kindex-1行的数据
                df4 = df.loc[:kindex-1].reset_index(drop=True)
                # 删除DataFrame中第7列之后的所有列
                df4.drop(columns=df4.columns[6:], inplace=True)
                # 重命名DataFrame的列名
                df4.columns = ['100 Most Traded Stocks YTD', 'Stock Coce', 'YTD Avg Daily Turnover (S$M)', 'YTD Institution Net Buy (+) /Net Sell (-) (S$M)', 'Past 5 Sessions Institution Net Buy (+) / Net Sell (-) (S$M)', 'Sector']
                # 将日期转换为周日期
                df4['Date'] = convert_week_date(sdate)
                # 将DataFrame添加到列表中
                df4s.append(df4)
                # 将DataFrame保存为Excel文件
                # df3.to_excel('df3.xlsx', index=False)
    # 将列表中的所有DataFrame合并为一个DataFrame
    ndf4 = pd.concat(df4s, ignore_index=True, axis=0)
    # 将合并后的DataFrame保存为Excel文件
    ndf4.to_excel('output\\df4.xlsx', index=False)
    # 返回合并后的DataFrame
    return ndf4


def fetch(method, *args, **kwargs):
    # 定义一个计数器，用于记录重试次数
    count = 0
    # 当重试次数小于10时，执行以下代码
    while count < 10:
        try:
            # 如果请求的文件是.xlsx格式，则返回请求的内容
            if '.xlsx' in args[0]:
                res = requests.request(method, *args, **kwargs).content
            # 否则，返回请求的json数据
            else:
                res = requests.request(method, *args, **kwargs).json()
            # 返回请求结果
            return res
        # 捕获异常
        except:
            # 打印重试信息
            print('retry...')
            # 计数器加1
            count += 1
    # 如果重试次数达到10次，执行以下代码
    else:
        # 打印请求参数
        print(kwargs)
        # 打印重试次数达到上限的信息
        print(f'已达到重试次数')
        # 如果请求的文件是.xlsx格式，则返回None，否则返回空字典
        res = None if '.xlsx' in args[0] else {}
        # 返回请求结果
        return res

# 定义一个函数，用于下载xlsx文件
def download_xlsx(url):
    # 发送GET请求，获取url对应的文件
    res = fetch('GET', url, headers=headers)
    # 如果请求失败，打印错误信息并返回
    if not res:
        print(f'{url}下载失败')
        return
    # 获取url中的文件名
    name = url.split('/')[-1]
    # 将文件保存到sgx文件夹中
    with open(f'sgx\\{name}', 'wb') as f:
        f.write(res)
    # 打印下载成功信息
    print(f"{url}下载成功")



# 定义一个函数，用于获取xlsx文件
def get_xlsxes():
    # 初始化偏移量为0
    offset = 0
    # 初始化一个空列表，用于存储xlsx文件的url
    urls = []
    # 无限循环，直到没有更多的xlsx文件
    while True:
        # 定义api的url
        api = 'https://api2.sgx.com/content-api'
        # 定义请求参数
        params = {
            'queryId': 'f6241666a335933a0ac9df97e14bfa23771b887c:funds_flow_reports_list',
            'variables': f'{{"limit":100,"offset":{offset},"reportType":"203","reportTypeFilterEnabled":true,"lang":"EN"}}',
        }
        # 发送请求
        res = fetch('GET', api, params=params, headers=headers)
        # 获取返回的数据
        ls = res.get('data', {}).get('list', {}).get('results', [])
        # 如果没有数据，则跳出循环
        if not ls:
            break
        # 遍历数据
        for data in ls:
            # 获取xlsx文件的url
            file_url = data.get('data', {}).get('report', {}).get('data', {}).get('file', {}).get('data', {}).get('url')
            # 解码url
            url = unquote(file_url)
            # 如果url中不包含.xlsx或者不包含Week of，则跳过
            if '.xlsx' not in url or 'Week of' not in url:
                continue
            # 将url添加到列表中
            urls.append(url)
        # 偏移量加100
        offset += 100
    # 使用线程池，并发下载xlsx文件
    with ThreadPoolExecutor(20) as pool:
        for url in urls:
            pool.submit(download_xlsx, url)


def main():
    # 调用get_xlsxes函数，获取xlsx文件
    get_xlsxes()
    # 调用parse_weekly函数，解析周报数据，返回df1和df2
    df1, df2 = parse_weekly()
    # 调用parse_sti函数，解析sti数据，返回df3
    df3 = parse_sti()
    # 调用parse_100函数，解析100数据，返回df4
    df4 = parse_100()
    print(f'Completed!')

if __name__ == '__main__':
    main()
   
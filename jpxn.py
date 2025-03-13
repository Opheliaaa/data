import requests
import os
import re
from lxml import etree
from concurrent.futures import ThreadPoolExecutor

if not os.path.exists('jpx'):
    os.mkdir('jpx')


headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    # 'Cookie': '_gcl_au=1.1.30382986.1727054909; __utma=139475176.817629616.1727054909.1727054909.1727054909.1; __utmc=139475176; __utmz=139475176.1727054909.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); __utmt_UA-59804733-1=1; __utmb=139475176.1.10.1727054909; _ga_PMR8JWDNL4=GS1.1.1727054908.1.0.1727054908.60.0.0; _ga=GA1.1.1600418302.1727054909; _ga_LMSH7DKWD8=GS1.1.1727054908.1.0.1727054908.0.0.0; _fbp=fb.2.1727054909239.116696622375205067',
    'Pragma': 'no-cache',
    'Referer': 'https://www.jpx.co.jp/english/markets/statistics-equities/investor-type/index.html',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-User': '?1',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
    'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Google Chrome";v="128"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
}


def fetch(method, *args, **kwargs):
    count = 0
    while count < 5:
       try:
           if args[0].endswith('html'):
               res = requests.request(method, *args, **kwargs)
               res.encoding = 'utf-8'
               return res.text
           else:
               res = requests.request(method, *args, **kwargs).content
               return res
       except Exception as e:
           print(e)
           count += 1
    else:
        print(kwargs)
        print(f'已达到重试次数')


def download_xlsx(link, name):
    dres = fetch('GET', link, headers=headers, timeout=40)
    if not dres:
        print(f'{name}获取失败')
        return
    with open(f'jpx\\{name}.xlsx', 'wb') as f:
        f.write(dres)
    print(f'{name}下载成功')


def get_data(index):
    links = []
    index = index.zfill(2)
    url = f'https://www.jpx.co.jp/english/markets/statistics-equities/investor-type/00-00-archives-{index}.html'
    res = fetch('GET', url, headers=headers, timeout=40)
    if not res:
        print(f'{index}获取失败')
        return
    html = etree.HTML(res)
    ls = html.xpath('//table[@class="overtable"]/tr')[1:]
    for data in ls:
        dt = data.xpath('./td[1]/text()')[0]
        year = re.findall(r'\d{4}', dt)[0]
        try:
            md = re.findall(r'（(.*?)）', dt)[0].replace(' ', '').replace('/', '-').replace('.', '-')
        except:
            try:
                md = re.findall(r'\((.*?)\)', dt)[0].replace(' ', '').replace('/', '-').replace('.', '-')
            except:
                print(f'{dt}获取失败')
                continue
        name = f'{year}-{md}'
        link = 'https://www.jpx.co.jp' + data.xpath('./td[last()]/a/@href')[0]
        print(link)
        links.append([link, name])

    with ThreadPoolExecutor(20) as pool:
        for link, name in links:
            pool.submit(download_xlsx, link, name)


def main():
    for index in range(11):  # 2014对应0,......2024对应10
        get_data(str(index))


if __name__ == '__main__':
    main()

# quanbull
stock quantity anaylis  tool  股票量化分析工具
builddbpro.py   是 调用 tushare pro  接口， 建立 本地日线行情数据库的 工具。
使用  步骤
1.  按  https://tushare.pro/document/1?doc_id=39 所说的 方法， 获取 tushare token,将代码中
ts.set_token('0000000000000000000000000000000000000000000000000000')
的 的0  字符串 换成 自己的 token

2. 将  dbname = 'C:\\good\\mypanel\\tudfdb.db'  修改成自己的 dbname 

3. 将  updateallstocks_bydataframe(4000, dbname） 中的  4000， 修改成 想 抓取的  历史行情天数， 就是 从现在以前多少天的行情


taselector.py 利用已经建立的行情  数据库 计算 技术指标的 程序。
需要 先安装  ta-lib , 在 anaconda  环境下， 执行以下，经试验 可以
conda install -c quantopian ta-lib 
建议 先在  anaconda 环境下，建一个 虚拟环境， 不会影响到 原来程序的运行


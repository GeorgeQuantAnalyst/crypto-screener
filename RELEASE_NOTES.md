# Release notes

## Version 1.1.1
Available since: 30.11.2022

```
New feature
```
* [efae0c0](https://github.com/GeorgeQuantAnalyst/crypto-screener/commit/efae0c02e003761095a3cdf4fe8346366a1f1d41) - Implemented support find intraday 1h buyer and seller imbalances

```
Improvement
```
* [fc63c0b](https://github.com/GeorgeQuantAnalyst/crypto-screener/commit/fc63c0bc6ec6595c38d47f6bdb6ea39ad2aeb2f5) - Implemented step  CreateViewsStep
* [d32db6d](https://github.com/GeorgeQuantAnalyst/crypto-screener/commit/d32db6d69bdac8971afb538190da0fd2da11748e) - Performance improve app: change save items into list instead concat df
* [6d821e5](https://github.com/GeorgeQuantAnalyst/crypto-screener/commit/6d821e569a5efd2cc83f4e48e62e92ccd5270f52) - Removed save buyer and seller interest imb to db

```
Bug
```
* [ec1c432](https://github.com/GeorgeQuantAnalyst/crypto-screener/commit/ec1c432b019c9a4b4e3c68d4c3b23a9fa2411124) - Phemex futures ticker update and set Binance BTC as reference


## Version 1.1.0
Available since: 27.10.2022

```
New feature
```
* [08da768](https://github.com/GeorgeQuantAnalyst/crypto-screener/commit/08da768bde2c184d73223fc52ee8bda1f31f61fd) - Create Imbalance screener
* [8a98dee](https://github.com/GeorgeQuantAnalyst/crypto-screener/commit/8a98deea700b2057a20bd8d3c80e973e80adbf57) -  Create data downloader step and connect other steps to load data from database
* [14652a2](https://github.com/GeorgeQuantAnalyst/crypto-screener/commit/14652a227a1b1a0bcf26a70b35d0c5ad2eb3fbb3) - Created sql select for find daily buyer and seller imbalances
* [3793cd3](https://github.com/GeorgeQuantAnalyst/crypto-screener/commit/3793cd37707cb086f0eabf952f6d8eee0c479736) - Filtered processed imbalances in db select
* [266acc1](https://github.com/GeorgeQuantAnalyst/crypto-screener/commit/266acc188e5768bb086ef262dc0240b1804a5872) - Created LoadProcessedImbalancesStep

```
Improvement
```
* [bf24f0d](https://github.com/GeorgeQuantAnalyst/crypto-screener/commit/bf24f0de091337250f094786daad8f86e56d568e) - Refactoring code for add new steps
* [a438010](https://github.com/GeorgeQuantAnalyst/crypto-screener/commit/a43801067cbf3ee467ffde48155a44e8b02ea286) - Unification of column names in dataseries (database)
* [67b435a](https://github.com/GeorgeQuantAnalyst/crypto-screener/commit/67b435a10b43ed04f8419bb3b2a7e438d85f57bf) - Faster download for PhemexFutures
* [58e6db4](https://github.com/GeorgeQuantAnalyst/crypto-screener/commit/58e6db41cccfcec6d4f8518ce77bf43b0622ddd8) - Clean code - converted if elif to match case
* [cf34a8a](https://github.com/GeorgeQuantAnalyst/crypto-screener/commit/cf34a8ac7b713ba3da6d2b0ecd1758e2a07c3e32) - added warning message if download not actual ohlc from exchange

```
Bug
```
* [8758850](https://github.com/GeorgeQuantAnalyst/crypto-screener/commit/8758850e277beff9252e336f26d77ff27b39f5de) - Removed support for SimpleFx (us equities) 


## Version 1.0.0
Available since: 1.10.2022

```
New feature
```
* [653fb](https://github.com/GeorgeQuantAnalyst/crypto-screener/commit/653fb04224c40670a01fa527c94b3b76379179eb) -
  Implemented app logic for Phemex futures screening
* [ee391](https://github.com/GeorgeQuantAnalyst/crypto-screener/commit/ee39115dbd56e83d75590511cad60a7deb5f81d3) - Added
  support for Binance spot screening
* [11d06](https://github.com/GeorgeQuantAnalyst/crypto-screener/commit/11d06700138020b9f412544bdd3f007cfb61bd21) - Added
  support for Okx exchange and SimpleFx screening
* [1cdb0](https://github.com/GeorgeQuantAnalyst/crypto-screener/commit/1cdb054194ea63831abff1d0bf5209d32f1bdfc4) - Implemented correlation with BTC

```
Improvement
```
* [5b5f5](https://github.com/GeorgeQuantAnalyst/crypto-screener/commit/5b5f5cd424f48e5c4660263c02d29561d9c544ce) - Added
  stop on first error in bash scripts

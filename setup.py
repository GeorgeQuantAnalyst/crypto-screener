import os

from setuptools import setup, find_packages

NAME = "crypto-screener"
DESCRIPTION = "Application for calculating the ratings based on the indicators (SMA 20, SMA 50, SMA 200), oscillators (RSI) and volatility (ATRW%) for Phemex futures and Kucoin spot cryptocurrencies."
AUTHOR = "Jiri"
URL = ""
VERSION = None

about = {}

with open(
        os.path.join(os.path.dirname(__file__), "requirements.txt"), "r"
) as fh:
    requirements = fh.readlines()

root = os.path.abspath(os.path.dirname(__file__))

if not VERSION:
    with open(os.path.join(root, "crypto_screener", "__version__.py")) as f:
        exec(f.read(), about)
else:
    about["__version__"] = VERSION

setup(
    name=NAME,
    version=about["__version__"],
    license="MIT",
    description=DESCRIPTION,
    long_description_content_type="text/markdown",
    AUTHOR=AUTHOR,
    url=URL,
    keywords=["Algo-trading", "Phemex"],
    install_requires=[req for req in requirements],
    packages=find_packages(exclude=("tests",)),
    classifiers=[
        "Intended Audience :: Developers",
        "Intended Audience :: Financial and Insurance Industry",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
    python_requires=">=3.6",
)

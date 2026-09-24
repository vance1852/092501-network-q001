"""城市地下管网服务的本地安装入口。"""
from setuptools import find_packages, setup
setup(name="urban-network-ops", version="0.1.0", description="城市地下管网安全监测与应急调度服务", long_description=open("README.md", encoding="utf-8").read(), long_description_content_type="text/markdown", package_dir={"": "src"}, packages=find_packages("src"), python_requires=">=3.11")

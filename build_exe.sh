#!/bin/bash
# 检查是否安装了 pyinstaller
if ! command -v pyinstaller &> /dev/null
then
    echo "pyinstaller 未安装，正在安装..."
    pip3 install pyinstaller
fi

echo "开始打包 CRM 系统..."
pyinstaller --noconfirm --onefile --windowed --name "CRM_Pro" "crm_app.py"
echo "打包完成！"

@echo off
call conda activate fb_tool
python main.py -m recharge -s %1
call conda deactivate
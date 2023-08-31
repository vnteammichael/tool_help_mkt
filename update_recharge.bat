@echo off
call conda activate fb_tool
python main.py -m recharge
call conda deactivate
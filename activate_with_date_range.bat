@echo off
call conda activate fb_tool
python main.py -s %1 -e %2
call conda deactivate
@echo off
chcp 65001
setlocal enabledelayedexpansion

set "Src=D:\Projects\airflow-docker\downloads"
set "Dst=D:\Data\test"

echo 正在比對 %Src% 和 %Dst% ...

set "updated=0"
set "failed=0"

for /r "%Src%" %%F in (*) do (
    set "srcFile=%%F"
    set "dstFile=!srcFile:%Src%=%Dst%!"
    
    if not exist "!dstFile!" (
        echo 複製 "!srcFile!" 到 "!dstFile!"
        mkdir "!dstFile:~0,-1!\.." 2>nul
        copy "!srcFile!" "!dstFile!" >nul
        if !errorlevel! equ 0 (
            set /a "updated+=1"
        ) else (
            set /a "failed+=1"
        )
    )
)

if %updated% gtr 0 (
    msg * "已更新"
) else if %failed% gtr 0 (
    msg * "失敗"
) else (
    msg * "無更新"
)

echo 完成。
echo 更新: %updated%
echo 失敗: %failed%

endlocal

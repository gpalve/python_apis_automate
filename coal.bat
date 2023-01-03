for /f "delims=" %%F in ('dir C:\Users\shiva\Downloads\dailyCoal*.xlsx /b') do set file=%%F
echo %file%

cd C:\Users\shiva\Desktop\CoalData_Python

move C:\Users\shiva\Downloads\%file% C:\Users\shiva\Desktop\CoalData_Python
python dc_convert.py %file%

echo 'successfully saved to CoalData_Python folder on desktop'
pause
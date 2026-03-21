@echo off
REM Activate the virtual environment
call ..\.venv\Scripts\activate

REM Run the Python script
python -m job.main_pipeline

cmd /k

REM Optional: Pause the window to see output
REM pause

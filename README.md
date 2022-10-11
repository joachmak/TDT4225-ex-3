# TDT4225, Very Large Distributed Data Volumes
Graded assignments for TDT4225

Created by Marit Lindstad, Jørgen Katralen and Joachim Maksim

### How to run
1. Create a virtual environment by running `python3 -m venv ./venv` in the project root.
2. Activate the virtual environment with `source <venv-path>/bin/activate` on mac and simply `<venv-path>/Scripts/activate` on Windows.
3. Install requirements from requirements.txt (in root) with `pip3 install -r requirements.txt` (if run from root)
4. Copy `.env.example` and name the copy `.env`. DB_PASS is the password to our gr24 database. You can reconfigure `utils/db_connector.py` to a different host, and add the password to your db in `DB_PASS` in `.env`. `DATASET_ROOT_PATH` should point to the dataset folder in your file system.
5. If you want to wipe and re-insert data, run `python3 task_1.py`. If you just want to run the queries for task 2, run `python3 task_2.py`

#!/bin/bash

cd /<YOUR PATH>

python3 -m venv meshtastic_env
source meshtastic_env/bin/activate
pip install meshtastic >/dev/null
pip install fastapi uvicorn
pip install 'uvicorn[standard]'


cd /<YOUR PATH>
python3 -m uvicorn app:app --host 0.0.0.0 --port 8000 --reload
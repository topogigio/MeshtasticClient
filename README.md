# Meshtastic Serial Client

[![Version](https://shields.io)](https://github.com)
[![License](https://shields.io)](https://creativecommons.org)
[![Python](https://shields.io)](https://python.org)
[![JavaScript](https://shields.io)](https://developer.mozilla.org/)

**Meshtastic Serial Client** is a cross-platform client application (Windows, macOS, Linux) designed to interface with Meshtastic radio nodes via serial connection. The application combines a powerful Python backend with a modern and intuitive web-based user interface.

Developed by **Andrea Marotta - IU0CRY**.

---

## 🚀 Key Features

- **Cross-platform**: 100% compatible with Windows, macOS, and Linux.
- **Automatic Port Detection**: Automatically identifies the device's serial port (usbmodem, COM, ttyACM/ttyUSB).
- **Smart Reconnection**: Handles hardware disconnects with an advanced cooldown and polling system to restore the connection without restarting the app.
- **Integrated Traceroute**: Asynchronous MeshTraceService to monitor packet paths on the mesh network.
- **Real-Time Web Interface**: FastAPI backend with WebSocket support for instant and seamless updates of node data and messages.
- **Automatic Browser Launch**: Automatically launches the GUI in your preferred browser (optimized for Google Chrome).

---

## 🛠️ Technologies Used

- **Backend**: Python 3.x, FastAPI, Uvicorn, PyPubSub.
- **Radio Libraries**: `meshtastic` (Python API), Google Protobuf.
- **Serial Communication**: `pyserial`.
- **Frontend**: JavaScript (HTML5/CSS3) integrated via WebSocket.

---

## 📦 Installation and Requirements

### Prerequisites
Make sure you have **Python 3.14+** installed on your system.

### 1. Clone the repository
```bash
git clone https://github.com/topogigio/MeshtasticClient.git
cd MeshtasticClient
```

### 2. Install dependencies
Install the required Python packages using `pip`:
```bash
pip install fastapi uvicorn pyserial meshtastic Pypubsub google-protobuf
```

---

## 💻 Usage

1. Connect your Meshtastic device (e.g., Heltec, T-Beam, LilyGO) to your computer via USB cable.
2. Run the application:
```bash
python app.py BTW see file for MacOS .command
```
3. The program will detect the correct port, start the local server, and automatically open a browser tab at:
`http://127.0.0.1:8000/`

---

## ⚠️ ATTENTION
The program isn't fully tested on Windows/Linux systems
You can make a trace route before send message to know if node is online, use it carefully, it produce hi impact LoRa traffic.

---

## 📄 License and Terms of Use

This software is licensed under the Creative Commons Attribution-NonCommercial 4.0 International (CC BY-NC 4.0) license.

See the `Licence.txt` file for the full legal text.

### Short Terms:
1. **Attribution (BY)**: You must give appropriate credit to the author (**Andrea Marotta - IU0CRY**), provide a link to the license, and indicate if changes were made.
2. **NonCommercial (NC)**: You may not use this material or any portion of it for commercial purposes.

**Disclaimer**: The software is provided "as is," without warranties of any kind. The author is not responsible for any damages or problems resulting from the use of the program.

---

## 📬 Contact and Support

- **Author**: Andrea Marotta (IU0CRY)
- **Bug Report**: Open an (https://github.com/topogigio/MeshtasticClient) directly on this GitHub repository.



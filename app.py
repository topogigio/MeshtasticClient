# ----------------------------------------------------------------------------------------------
#                      -+* Meshtastic Serial Client *+-
#                 Client app for win/mac/linux - Version 0.99
# 
#                     Developed by Andrea Marotta - IU0CRY
# 
#              Main backend programming language Python Ver. 3.xx
#                             Frontend JavaScript
# 
#                        Distributed under the license: 
#              CC BY-NC Creative Commons Attribution-NonCommercial
#                            See file Licence.txt
# ----------------------------------------------------------------------------------------------
# Copyright (c) 2026 Andrea Marotta IU0CRY
# 
# Distributed under the Creative Commons Attribution-NonCommercial 4.0 International License.
# 
# TERMS OF USE:
# 1. Attribution (BY): You must give appropriate credit, provide a link to the license, and 
# indicate if changes were made. You may do so in any reasonable manner, but not in any way 
# that suggests the licensor endorses you or your use.
# 2. NonCommercial (NC): You may not use the material for commercial purposes.
# 
# DISCLAIMER OF WARRANTY:
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING 
# BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND 
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, 
# DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, 
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
# 
# To view a copy of the full legal code of this license, visit:
# https://creativecommons.org
# ----------------------------------------------------------------------------------------------
import asyncio
import threading
import platform
import serial
import serial.tools.list_ports
import json
import re
import time
import meshtastic
import meshtastic.serial_interface
import meshtastic.tcp_interface  # <--- AGGIUNTO SUPPORTO TCP
import pubsub.pub
import base64
import os
import webbrowser

from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, Response, FileResponse  
from fastapi.middleware.cors import CORSMiddleware
from pubsub import pub
from google.protobuf.json_format import MessageToDict
from serial import SerialException
from meshtastic.serial_interface import SerialInterface
from meshtastic.tcp_interface import TCPInterface  # <--- AGGIUNTO SUPPORTO TCP

# ==============================================================================================
# LETTURA CONFIGURAZIONE DA VARIABILI D'AMBIENTE (Risolto conflitto Uvicorn)
# ==============================================================================================
CONFIG_MODE = os.environ.get("MESHTASTIC_MODE", "serial")  
CONFIG_TARGET = os.environ.get("MESHTASTIC_TARGET", "").strip()


def open_browser():
    try:
        time.sleep(1.5)
        # prova Chrome se registrato
        chrome = webbrowser.get("chrome")
        chrome.open("http://127.0.0.1:8000/")
        print(f"Browser aperto usando Chrome su http://127.0.0.1:8000/")
        #return True
    except:
        # fallback browser di sistema
        webbrowser.open("http://127.0.0.1:8000/")
        print(f"Browser aperto usando default su http://127.0.0.1:8000/")
        #return False
 
async def handle_node_reconnect(target_path, websocket):
    global iface_global
    print("Fase 1: Cooldown hardware (12 secondi)...")
    await asyncio.sleep(12.0)
    
    print(f"Fase 2: Tentativo di riconnessione a {target_path}...")
    max_attempts = 20
    
    for attempt in range(max_attempts):
        try:
            loop = asyncio.get_running_loop()
            if CONFIG_MODE == "wifi":
                new_iface = await loop.run_in_executor(
                    None, lambda: TCPInterface(hostname=target_path)
                )
            else:
                new_iface = await loop.run_in_executor(
                    None, lambda: SerialInterface(devPath=target_path, noProto=False)
                )
            
            if new_iface.myInfo:
                iface_global = new_iface
                print("Fase 3: Riconnessione riuscita!")
                
                # ====================================================================
                # AGGIUNTO: SPINGI I NODI DALLA CACHE DOPO IL REBOOT DI RETE
                # ====================================================================
                if new_iface.nodes:
                    print(f"[*] Ripristino di {len(new_iface.nodes)} nodi in corso...")
                    for node_id, node_data in new_iface.nodes.items():
                        pacchetto_iniziale = {
                            "type": "nodeinfo",
                            "from": str(node_id),
                            "node": node_data,
                            "radio": node_data.get("radio", {})
                        }
                        if "raw" in pacchetto_iniziale["node"]: 
                            del pacchetto_iniziale["node"]["raw"]
                        
                        # Li inviamo direttamente al server websocket attivo
                        await websocket.send_text(json.dumps(pacchetto_iniziale))
                # ====================================================================

                # Avvisa l'interfaccia grafica che tutto è pronto e sbloccato
                await websocket.send_text(json.dumps({
                    "type": "node_ready",
                    "message": "Nodo riconnesso con successo!"
                }))
                return
        except Exception:
            print(f"Tentativo {attempt + 1} fallito, riprovo...")
            await asyncio.sleep(1.5)
            
    await websocket.send_text(json.dumps({
        "type": "node_timeout",
        "message": "Impossibile riconnettersi al nodo automaticamente."
    }))



class MeshTraceService:

    def __init__(self, iface):

        self.iface = iface
        self.result = None
        self.done = False

    def on_receive(self, packet, interface):

        if packet.get('decoded', {}).get('portnum') == 'TRACEROUTE_APP':
            self.result = True
            self.done = True

    def run(self, target, hop=3, timeout=10):

        self.result = None
        self.done = False

        pubsub.pub.subscribe(self.on_receive, "meshtastic.receive")

        try:
            print("TRACE 2")
            self.iface.sendTraceRoute(
                dest=target,
                hopLimit=hop,
                channelIndex=0
            )
            print("TRACE 2")

        except Exception as e:
            return {"ok": False, "error": str(e)}

        start = time.time()

        while time.time() - start < timeout:
            if self.done:
                break
            time.sleep(0.2)

        pubsub.pub.unsubscribe(self.on_receive, "meshtastic.receive")

        if self.result:
            return {"ok": True}
        else:
            return {"ok": False, "error": "timeout"}

# Gestore globale delle connessioni WebSocket
class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, data: dict):
        message = json.dumps(data)
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except Exception:
                pass

manager = ConnectionManager()
seriale_globale = None
iface_global = None

def trova_usbmodem():
    sistema_op = platform.system()
    porte = list(serial.tools.list_ports.comports())
    if sistema_op == "Darwin":
        for porta in porte:
            if "usbmodem" in porta.device:
                return porta.device
    elif sistema_op == "Windows":
        for porta in porte:
            if "COM" in porta.device:
                return porta.device
    elif sistema_op == "Linux":
        for porta in porte:
            if "ttyACM" in porta.device or "ttyUSB" in porta.device:
                return porta.device
    return None

def fmt_node_id(nid):
    s = str(nid) if nid is not None else ""
    if not s:
        return ""
    return s

def lettura_nodo(target, loop):
    try:
        if CONFIG_MODE == "wifi":
            interface = TCPInterface(hostname=target)
            print(f"[*] Connessione TCP stabilita con successo su {target}")
        else:
            interface = SerialInterface(target)
            print(f"[*] Connessione SERIALE stabilita con successo su {target}")

        global iface_global
        iface_global = interface

        # -------------------------
        # CACHE NODI
        # -------------------------
        nodi = {}

        def get_node_info(node_id):
            if not node_id:
                return None

            if node_id in nodi:
                return nodi[node_id]

            nodo = {
                "id": str(node_id),
                "id_display": str(node_id),
                "longName": None,
                "shortName": None,
                "role": None,
                "model": None,
                "radio": {},
                "messages": []
            }

            nodi[node_id] = nodo
            return nodo
            
        def sanitize(obj):
            if isinstance(obj, dict):
                return {k: sanitize(v) for k, v in obj.items() if k != "raw"}
            if isinstance(obj, list):
                return [sanitize(x) for x in obj]
            return obj

        # -------------------------
        # SUBSCRIBE MESH
        # -------------------------
        def onReceive(packet, interface):
            try:
                decoded = packet.get("decoded", {}) or {}
                portnum = decoded.get("portnum")
        
                user = decoded.get("user") or {}
                user_id = user.get("id")
        
                pkt_from_id = packet.get("fromId")
                pkt_from = packet.get("from")
        
                # -------------------------
                # NODE KEY UNICO
                # -------------------------
                node_id = user_id or pkt_from_id or pkt_from
                if not node_id:
                    return
        
                node_id = str(node_id)
        
                nodo = get_node_info(node_id)
        
                if "messages" not in nodo:
                    nodo["messages"] = []
                if "radio" not in nodo:
                    nodo["radio"] = {}
        
                nodo["id"] = node_id
                nodo["id_display"] = user_id or node_id
        
                # -------------------------
                # RADIO UPDATE (sempre valido)
                # -------------------------
                def update_radio(pkt):
                    rssi = pkt.get("rxRssi")
                    snr = pkt.get("rxSnr")
                    hops = pkt.get("hopLimit") or pkt.get("hopStart")
        
                    if rssi is not None:
                        nodo["radio"]["lastRssi"] = rssi
                    if snr is not None:
                        nodo["radio"]["lastSnr"] = snr
                    if hops is not None:
                        nodo["radio"]["lastHop"] = hops
        
                update_radio(packet)
        
                # =========================================================
                # 🧑 NODEINFO
                # =========================================================
                if portnum == "NODEINFO_APP":
                
                    print("\n===== NODEINFO RAW PACKET =====")
                    print(packet)
                    print("==============================\n")
                    
                    user = decoded.get("user", {}) or {}
                
                    node_id = user.get("id") or pkt_from_id or pkt_from
                    if not node_id:
                        return
                
                    # aggiorna cache nodo
                    nodo = get_node_info(node_id)
                
                    if user.get("longName"):
                        nodo["longName"] = user["longName"]
                    if user.get("shortName"):
                        nodo["shortName"] = user["shortName"]
                    if user.get("role"):
                        nodo["role"] = user["role"]
                    if user.get("hwModel"):
                        nodo["model"] = user["hwModel"]
                
                    nodo["updated"] = True
                
                    # radio opzionale
                    nodo.setdefault("radio", {})
                    nodo["radio"]["lastRssi"] = packet.get("rxRssi")
                    nodo["radio"]["lastSnr"] = packet.get("rxSnr")
                
                    msg_type = "nodeinfo"
                
                    pacchetto = {
                        "type": msg_type,
                        "from": node_id,
                        "node": {
                            **nodo,
                            "user": user
                        },
                        "radio": nodo.get("radio", {})
                    }
                
                    print("\n===== NODEINFO AL FRONTEND =====")
                    print(pacchetto)
                    print("==============================\n")
                    
                    pacchetto = sanitize(pacchetto)
               
                    asyncio.run_coroutine_threadsafe(
                        manager.broadcast(pacchetto),
                        loop
                    )
                
                    return pacchetto
                            
                # =========================================================
                # 💬 CHAT
                # =========================================================
                if portnum == "TEXT_MESSAGE_APP":

                    print("\n===== CHAT RAW PACKET =====")
                    print(packet)
                    print("==============================\n")
                    
                    message = decoded.get("text")
                    if message is None:
                        try:
                            message = decoded.get("payload", b"").decode("utf-8", errors="ignore")
                        except Exception:
                            message = str(decoded.get("payload"))
        
                    nodo["messages"].append({
                        "text": message or "",
                        "time": packet.get("rxTime")
                    })
        
                    pacchetto = {
                        "type": "chat",
                        "from": node_id,
                        "text": message,
                        "node": nodo,
                        "radio": nodo.get("radio", {}),
                        "time": packet.get("rxTime")
                    }
        
                    print("\n===== CHAT AL FRONTEND =====")
                    print(pacchetto)
                    print("==============================\n")

                    asyncio.run_coroutine_threadsafe(
                        manager.broadcast(pacchetto),
                        loop
                    )
        
                    return pacchetto
        
                # =========================================================
                # 📡 GENERIC PACKET
                # =========================================================
                
                pacchetto = {
                    "type": "radio""",
                    "from": node_id,
                    "node": nodo,
                    "decoded": decoded,
                    "radio": nodo.get("radio", {}),
                    "time": packet.get("rxTime")
                }
        
                asyncio.run_coroutine_threadsafe(
                    manager.broadcast(pacchetto),
                    loop
                )
        
                return pacchetto
        
            except Exception as e:
                print(f"Error processing packet: {e}")
        
        pub.subscribe(onReceive, "meshtastic.receive")

   	    # -------------------------
        # SEND LOOP
        # -------------------------
        def send_message(message):
            global iface_global
            iface_global.sendText(message)


        while True:
            text = input("")
            send_message(text)
			
        while iface_global and not iface_global.noProto:
            time.sleep(1)

        print("[!] L'interfaccia Meshtastic ha chiuso la connessione.")
    except Exception as e:
        print(f"[!] Errore di inizializzazione sul target {target}: {e}")

@asynccontextmanager
async def lifespan(app: FastAPI):
    target_connessione = None
    
    if CONFIG_MODE == "wifi":
        target_connessione = CONFIG_TARGET
        print(f"[*] Modalità Wi-Fi impostata su IP: {target_connessione}")
    elif CONFIG_MODE == "serial" and CONFIG_TARGET:
        target_connessione = CONFIG_TARGET
        print(f"[*] Modalità Seriale impostata su porta: {target_connessione}")
    else:
        print("[*] Nessun parametro specifico. Tento l'autodetect USB...")
        target_connessione = trova_usbmodem()
        
    if target_connessione:
        loop = asyncio.get_running_loop()
        thread = threading.Thread(target=lettura_nodo, args=(target_connessione, loop), daemon=True)
        thread.start()
        threading.Thread(target=open_browser, daemon=True).start()
    else:
        print("[!] Errore: Nessun dispositivo configurato o rilevato in rete/seriale.")
    yield


app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    # Trova il percorso esatto del file nella cartella corrente
    current_dir = os.path.dirname(os.path.abspath(__file__))
    favicon_path = os.path.join(current_dir, "favicon.ico")
    
    # Controlla se il file esiste davvero per evitare crash
    if os.path.exists(favicon_path):
        return FileResponse(favicon_path)
    
    # Se il file non esiste, risponde con un'immagine vuota senza andare in errore 500
    return Response(content=b"", media_type="image/x-icon")

@app.get("/")
async def get():
    return HTMLResponse(html_content)

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    
    try:
        # 🔥 INIT CANALI
        if iface_global:
            try:
                channels = []
                for ch in iface_global.localNode.channels:
                    channels.append({
                        "index": getattr(ch, "index", 0),
                        "name": getattr(
                            getattr(ch, "settings", None),
                            "name",
                            f"Channel {getattr(ch, 'index', 0)}"
                        )
                    })

                await websocket.send_text(json.dumps({
                    "type": "init",
                    "channels": channels
                }))
                print("[WS] INIT inviato")
            except Exception as e:
                print("[WS INIT ERROR]:", e)

        # 🔥 INVIA CONFIGURAZIONE
        if iface_global:
            try:
                config = {
                    "localConfig": MessageToDict(
                        iface_global.localNode.localConfig,
                        preserving_proto_field_name=True
                    ),
                    "moduleConfig": MessageToDict(
                        iface_global.localNode.moduleConfig,
                        preserving_proto_field_name=True
                    )
                }
                
                print("\n===== LOCALCONFIG RAW PACKET =====")
                print(json.dumps(
                    MessageToDict(
                        iface_global.localNode.localConfig,
                        preserving_proto_field_name=True
                    ),
                    indent=2
                ))
                print("==============================\n")
                
                print("\n===== MODULECONFIG RAW PACKET =====")
                print(json.dumps(
                    MessageToDict(
                        iface_global.localNode.moduleConfig,
                        preserving_proto_field_name=True
                    ),
                    indent=2
                ))
                print("==============================\n")
                
                await websocket.send_text(json.dumps({
                    "type": "config",
                    "config": config
                }))
                print("[WS] CONFIG inviata")
            except Exception as e:
                print("[CONFIG ERROR]", e)

        # ====================================================================
        # 🔥 NUOVO - AGGIUNTO: SPINGI I NODI DALLA CACHE ALL'APERTURA DEL WS
        # ====================================================================
        if iface_global and hasattr(iface_global, 'nodes') and iface_global.nodes:
            try:
                print(f"[*] Invio iniziale di {len(iface_global.nodes)} nodi dalla cache...")
                for node_id, node_data in iface_global.nodes.items():
                    # Sanatizziamo l'oggetto dai dati 'raw' per evitare problemi di serializzazione JSON
                    nodo_pulito = json.loads(json.dumps(node_data, default=str))
                    if "raw" in nodo_pulito:
                        del nodo_pulito["raw"]
                        
                    pacchetto_iniziale = {
                        "type": "nodeinfo",
                        "from": str(node_id),
                        "node": {
                            "id": str(node_id),
                            "id_display": nodo_pulito.get("user", {}).get("id", str(node_id)),
                            "longName": nodo_pulito.get("user", {}).get("longName"),
                            "shortName": nodo_pulito.get("user", {}).get("shortName"),
                            "role": nodo_pulito.get("user", {}).get("role"),
                            "model": nodo_pulito.get("user", {}).get("hwModel"),
                            "radio": {
                                "lastRssi": nodo_pulito.get("snr"), # Fallback snr/rssi se presenti
                                "lastSnr": nodo_pulito.get("snr")
                            },
                            "messages": [],
                            "user": nodo_pulito.get("user", {})
                        },
                        "radio": {}
                    }
                    
                    await websocket.send_text(json.dumps(pacchetto_iniziale))
                
                # Segnala al frontend che la sincronizzazione iniziale è completata
                await websocket.send_text(json.dumps({
                    "type": "node_ready",
                    "message": "Nodi caricati dalla cache!"
                }))
            except Exception as e:
                print("[WS NODES INIT ERROR]:", e)
        # ====================================================================
    
        # 🔁 LOOP MESSAGGI IN INGRESSO DAL FRONTEND
        while True:
            comando_web = await websocket.receive_text()
            # ... (Tutto il resto del tuo codice dentro il loop 'while True' rimane identico) ...
			


            try:
                msg = json.loads(comando_web)
            except Exception:
                msg = {"tipo": "chat", "messaggio": comando_web, "to": "broadcast"}

            if iface_global:

                try:

                    testo = msg.get("messaggio", "")
                    destinatario = msg.get("to", "broadcast")

                    # 🔍 TRACEROUTE + INVIO
                    if msg.get("tipo") == "traceroute":
                    
                        messaggio = msg.get("message", "")
                    
                        time.sleep(0.2)  # evita race Meshtastic
                    
                        result_holder = {"ok": False, "done": False}
                    
                        def run_trace():
                            try:
                                trace = MeshTraceService(iface_global)
                    
                                res = trace.run(
                                    target=destinatario,
                                    hop=3,
                                    timeout=10
                                )
                    
                                result_holder["ok"] = res.get("ok", False)
                    
                            except Exception as e:
                                print("[TRACE ERROR]", e)
                    
                            finally:
                                result_holder["done"] = True
                    
                    
                        print("TRACEROUTE START")
                    
                        t = threading.Thread(target=run_trace, daemon=True)
                        t.start()
                        t.join(timeout=12)
                    
                        print("TRACEROUTE END SAFE", result_holder)
                    
                        await websocket.send_text(json.dumps({
                            "type": "trace_result",
                            "ok": result_holder["ok"],
                            "dest": destinatario,
                            "message": messaggio
                        }))
                    
                        # 🚀 INVIO SOLO SE OK
                        if result_holder["ok"]:
                            iface_global.sendText(
                                messaggio,
                                destinationId=destinatario
                            )
                    
                        continue
                        await websocket.send_text(json.dumps({
                            "type": "trace_result",
                            "result": result
                        }))

                        # 🚀 INVIO SOLO SE OK
                        if result["ok"]:
                            iface_global.sendText(
                                messaggio,
                                destinationId=destinatario
                            )

                    # 🌐 BROADCAST
                    elif destinatario == "broadcast":
                        iface_global.sendText(testo)

                    # 📻 CANALE
                    elif destinatario.startswith("channel:"):
                        channel_index = int(destinatario.split(":")[1])

                        iface_global.sendText(
                            testo,
                            channelIndex=channel_index
                        )

                    # 👤 NODO
                    elif destinatario.startswith("node:"):
                        node_id = destinatario.split(":")[1]

                        iface_global.sendText(
                            testo,
                            destinationId=node_id
                        )

                    else:
                        print(f"[!] Destinatario non valido: {destinatario}")
                                                
                    if msg.get("type") == "get_config":
                        try:
                            config = iface_global.localNode.getConfig()
                    
                            await websocket.send_text(json.dumps({
                                "type": "config",
                                "config": config
                            }))
                        except Exception as e:
                            print("CONFIG GET ERROR:", e)
                        continue

                    if msg.get("type") == "set_config":
                        try:
                            cfg = msg.get("config", {})
                    
                            print("=== CONFIG PATCH MODE ===")
                    
                            # INIZIALIZZAZIONE OBBLIGATORIA DEI FLAG
                            # Questo evita l'errore "cannot access local variable"
                            lora_updated = False
                            network_updated = False
                            device_updated = False
                            position_updated = False
                            display_updated = False
                            power_updated = False
                            cfg = msg.get("config", {})
                            bluetooth_updated = False
                            security_updated = False
                            power_updated = False
                            mqtt_updated = False
                    
                            print("=== CONFIG PATCH MODE ===")
                    
                            # ==========================================
                            # 1. GRUPPO LORA (Provoca Reboot)
                            # ==========================================
                            if "lora" in cfg:
                                lora = cfg["lora"]
                                node_lora = iface_global.localNode.localConfig.lora
                    
                                # 1. Potenza di trasmissione
                                if "tx_power" in lora:
                                    node_lora.tx_power = int(lora["tx_power"])
                                    print(f"Predisposto tx_power: {lora['tx_power']}")
                                    lora_updated = True

                                # 2. Regione di frequenza (Gestisce sia stringhe che interi)
                                if "region" in lora:
                                    region_val = lora["region"]
                                    
                                    # Mappa ufficiale delle regioni Meshtastic (Stringa -> Int)
                                    region_map = {
                                        "UNSET": 0, "US": 1, "EU_433": 2, "EU_868": 3, 
                                        "CN": 4, "JP": 5, "ANZ": 6, "KR": 7, "TW": 8, 
                                        "RU": 9, "IN": 10, "UK_433": 11, "UK_868": 12,
                                        "LORA_24G": 13, "UA_433": 14, "UA_868": 15
                                    }
                                    
                                    if isinstance(region_val, str):
                                        # Converte in maiuscolo per evitare problemi di case-sensitivity
                                        region_val = region_map.get(region_val.upper(), 0)
                                        
                                    node_lora.region = int(region_val)
                                    print(f"Predisposto region: {node_lora.region} (da {lora['region']})")
                                    lora_updated = True

                                # 3. Modem Preset (Gestisce sia stringhe che interi)
                                if "modem_preset" in lora:
                                    preset_val = lora["modem_preset"]
                                    
                                    # Mappa ufficiale dei preset di banda Meshtastic (Stringa -> Int)
                                    preset_map = {
                                        "LONG_FAST": 0, "LONG_SLOW": 1, "VERY_LONG_SLOW": 2,
                                        "MEDIUM_SLOW": 3, "MEDIUM_FAST": 4, "SHORT_FAST": 5,
                                        "SHORT_SLOW": 6, "LONG_MODERATE": 7, "SHORT_TURBO": 8
                                    }
                                    
                                    if isinstance(preset_val, str):
                                        preset_val = preset_map.get(preset_val.upper(), 0)
                                        
                                    node_lora.modem_preset = int(preset_val)
                                    print(f"Predisposto modem_preset: {node_lora.modem_preset} (da {lora['modem_preset']})")
                                    lora_updated = True
                                    
                                # 4. Forza l'accensione della sezione radio (Booleano)
                                if "tx_enabled" in lora:
                                    node_lora.tx_enabled = bool(lora["tx_enabled"])
                                    print(f"Predisposto tx_enabled: {lora['tx_enabled']}")
                                    lora_updated = True

                                # 5. Abilitazione Hopping / Ripetitore (Booleano)
                                if "hop_limit" in lora:
                                    node_lora.hop_limit = int(lora["hop_limit"])
                                    print(f"Predisposto hop_limit: {lora['hop_limit']}")
                                    lora_updated = True

                                # 6. Larghezza di banda / Bandwidth (Intero, kHz, usato se modem_preset è personalizzato)
                                if "bandwidth" in lora:
                                    node_lora.bandwidth = int(lora["bandwidth"])
                                    print(f"Predisposto bandwidth: {lora['bandwidth']}")
                                    lora_updated = True

                                # 7. Spreading Factor (Intero, da 6 a 12, usato se modem_preset è personalizzato)
                                if "spreading_factor" in lora:
                                    node_lora.spreading_factor = int(lora["spreading_factor"])
                                    print(f"Predisposto spreading_factor: {lora['spreading_factor']}")
                                    lora_updated = True

                                # 8. Coding Rate (Intero, es. 5 per 4/5, 6 per 4/6, usato se personalizzato)
                                if "coding_rate" in lora:
                                    node_lora.coding_rate = int(lora["coding_rate"])
                                    print(f"Predisposto coding_rate: {lora['coding_rate']}")
                                    lora_updated = True

                                # 9. Frequenza di override manuale (Float, es. 868.125)
                                if "frequency_offset" in lora:
                                    node_lora.frequency_offset = float(lora["frequency_offset"])
                                    print(f"Predisposto frequency_offset: {lora['frequency_offset']}")
                                    lora_updated = True

                                # 10. Tempo di occupazione del canale prima di trasmettere (Intero, millisecondi)
                                if "override_duty_cycle" in lora:
                                    node_lora.override_duty_cycle = bool(lora["override_duty_cycle"])
                                    print(f"Predisposto override_duty_cycle: {lora['override_duty_cycle']}")
                                    lora_updated = True

                                # 11. Ignora MQTT (Booleano - se attivo, non inoltra i pacchetti locali verso i canali MQTT)
                                if "ignore_mqtt" in lora:
                                    node_lora.ignore_mqtt = bool(lora["ignore_mqtt"])
                                    print(f"Predisposto ignore_mqtt: {lora['ignore_mqtt']}")
                                    lora_updated = True

                                # 12. Numero di slot di backoff per evitare collisioni (Intero)
                                if "sx126k_override_dio2_rx_switch" in lora:
                                    node_lora.sx126k_override_dio2_rx_switch = bool(lora["sx126k_override_dio2_rx_switch"])
                                    print(f"Predisposto sx126k_override_dio2_rx_switch: {lora['sx126k_override_dio2_rx_switch']}")
                                    lora_updated = True
                    
                            # ==========================================
                            # 2. GRUPPO DEVICE (Nessun Reboot)
                            # ==========================================
                            if "device" in cfg:
                                device = cfg["device"]
                                node_device = iface_global.localNode.localConfig.device

                                if "role" in device:
                                    role_val = device["role"]
                                    role_map = {
                                        "CLIENT": 0, "REPEATER": 1, "ROUTER": 2, "ROUTER_CLIENT": 3,
                                        "SEED": 4, "TRACKER": 5, "TAK": 6, "CLIENT_MUTE": 7,
                                        "LOST_AND_FOUND": 8, "TAK_TRACKER": 9
                                    }
                                    if isinstance(role_val, str):
                                        role_val = role_map.get(role_val.upper(), 0)
                                    node_device.role = int(role_val)
                                    device_updated = True

                                if "button_gpio" in device:
                                    node_device.button_gpio = int(device["button_gpio"])
                                    device_updated = True

                                if "tz_offset" in device:
                                    node_device.tz_offset = int(device["tz_offset"])
                                    device_updated = True

                                if "reboot_count" in device:
                                    node_device.reboot_count = int(device["reboot_count"])
                                    device_updated = True

                            # ==========================================
                            # 3. GRUPPO POSITION (Nessun Reboot)
                            # ==========================================
                            if "position" in cfg:
                                position = cfg["position"]
                                node_position = iface_global.localNode.localConfig.position

                                if "position_broadcast_secs" in position:
                                    node_position.position_broadcast_secs = int(position["position_broadcast_secs"])
                                    position_updated = True

                                if "gps_enabled" in position:
                                    node_position.gps_enabled = bool(position["gps_enabled"])
                                    position_updated = True

                                if "gps_update_interval" in position:
                                    node_position.gps_update_interval = int(position["gps_update_interval"])
                                    position_updated = True

                                if "fixed_position" in position:
                                    node_position.fixed_position = bool(position["fixed_position"])
                                    position_updated = True

                            # ==========================================
                            # 4. GRUPPO DISPLAY (Nessun Reboot)
                            # ==========================================
                            if "display" in cfg:
                                display = cfg["display"]
                                node_display = iface_global.localNode.localConfig.display

                                if "screen_on_secs" in display:
                                    node_display.screen_on_secs = int(display["screen_on_secs"])
                                    display_updated = True

                                if "gps_format" in display:
                                    gps_val = display["gps_format"]
                                    gps_map = {"DEC": 0, "DMS": 1, "UTM": 2, "MGRS": 3, "OLC": 4}
                                    if isinstance(gps_val, str):
                                        gps_val = gps_map.get(gps_val.upper(), 0)
                                    node_display.gps_format = int(gps_val)
                                    display_updated = True

                                if "auto_screen_carousel_secs" in display:
                                    node_display.auto_screen_carousel_secs = int(display["auto_screen_carousel_secs"])
                                    display_updated = True

                            # ==========================================
                            # 5. GRUPPO NETWORK (Provoca Reboot)
                            # ==========================================
                            if "network" in cfg:
                                network = cfg["network"]
                                node_network = iface_global.localNode.localConfig.network

                                if "wifi_enabled" in network:
                                    node_network.wifi_enabled = bool(network["wifi_enabled"])
                                    network_updated = True

                                if "wifi_ssid" in network:
                                    node_network.wifi_ssid = str(network["wifi_ssid"])
                                    network_updated = True

                                if "wifi_psk" in network:
                                    node_network.wifi_psk = str(network["wifi_psk"])
                                    network_updated = True

                                if "address_mode" in network:
                                    # Correzione: estraiamo prima il valore dall'oggetto network
                                    addr_val = network["address_mode"]
                                    
                                    addr_map = {"DHCP": 0, "STATIC": 1}
                                    if isinstance(addr_val, str):
                                        addr_val = addr_map.get(addr_val.upper(), 0)
                                    node_network.address_mode = int(addr_val)
                                    network_updated = True

                            # ==========================================
                            # 4B. GRUPPO POWER (Nessun Reboot)
                            # ==========================================
                            if "power" in cfg:
                                power = cfg["power"]
                                node_power = iface_global.localNode.localConfig.power

                                if "is_power_saving" in power:
                                    node_power.is_power_saving = bool(power["is_power_saving"])
                                    power_updated = True

                                if "on_battery_shutdown_after_secs" in power:
                                    node_power.on_battery_shutdown_after_secs = int(power["on_battery_shutdown_after_secs"])
                                    power_updated = True

                                if "adc_multiplier_override" in power:
                                    node_power.adc_multiplier_override = float(power["adc_multiplier_override"])
                                    power_updated = True

                                if "wait_bluetooth_secs" in power:
                                    node_power.wait_bluetooth_secs = int(power["wait_bluetooth_secs"])
                                    power_updated = True

                                if "min_battery_v" in power:
                                    node_power.min_battery_v = float(power["min_battery_v"])
                                    power_updated = True

                                if "sds_secs" in power:
                                    node_power.sds_secs = int(power["sds_secs"])
                                    power_updated = True

                                if "ls_secs" in power:
                                    node_power.ls_secs = int(power["ls_secs"])
                                    power_updated = True

                            # ==========================================
                            # 4C. GRUPPO BLUETOOTH (Provoca Reboot)
                            # ==========================================
                            if "bluetooth" in cfg:
                                bluetooth = cfg["bluetooth"]
                                node_bt = iface_global.localNode.localConfig.bluetooth

                                if "enabled" in bluetooth:
                                    node_bt.enabled = bool(bluetooth["enabled"])
                                    bluetooth_updated = True

                                if "mode" in bluetooth:
                                    bt_mode_val = bluetooth["mode"]
                                    bt_mode_map = {"RANDOM_PIN": 0, "FIXED_PIN": 1, "NO_PIN": 2}
                                    if isinstance(bt_mode_val, str):
                                        bt_mode_val = bt_mode_map.get(bt_mode_val.upper(), 0)
                                    node_bt.mode = int(bt_mode_val)
                                    bluetooth_updated = True

                                if "fixed_pin" in bluetooth:
                                    node_bt.fixed_pin = int(bluetooth["fixed_pin"])
                                    bluetooth_updated = True

                            # ==========================================
                            # 4D. GRUPPO SECURITY (Provoca Reboot)
                            # ==========================================
                            if "security" in cfg:
                                security = cfg["security"]
                                node_sec = iface_global.localNode.localConfig.security

                                if "admin_key" in security:
                                    key_val = security["admin_key"]
                                    if isinstance(key_val, str):
                                        key_val = key_val.encode('utf-8')
                                    node_sec.admin_key = key_val
                                    security_updated = True

                                if "is_unencrypted" in security:
                                    node_sec.is_unencrypted = bool(security["is_unencrypted"])
                                    security_updated = True

                                if "debug_log_api_key" in security:
                                    node_sec.debug_log_api_key = str(security["debug_log_api_key"])
                                    security_updated = True

                            # ==========================================
                            # 4E. GRUPPO MQTT (Nessun Reboot)
                            # ==========================================
                            if "mqtt" in cfg:
                                mqtt = cfg["mqtt"]
                                node_mqtt = iface_global.localNode.moduleConfig.mqtt

                                # 1. Abilitazione globale del modulo (Booleano)
                                if "enabled" in mqtt:
                                    node_mqtt.enabled = bool(mqtt["enabled"])
                                    mqtt_updated = True

                                # 2. Indirizzo del Server Broker (Stringa, es. "mqtt.meshtastic.org")
                                if "address" in mqtt:
                                    node_mqtt.address = str(mqtt["address"])
                                    mqtt_updated = True

                                # 3. Username di autenticazione (Stringa)
                                if "username" in mqtt:
                                    node_mqtt.username = str(mqtt["username"])
                                    mqtt_updated = True

                                # 4. Password di autenticazione (Stringa)
                                if "password" in mqtt:
                                    node_mqtt.password = str(mqtt["password"])
                                    mqtt_updated = True

                                # 5. Topic radice per la pubblicazione (Stringa, default "msh")
                                if "root" in mqtt:
                                    node_mqtt.root = str(mqtt["root"])
                                    mqtt_updated = True

                                # 6. Inoltro dei pacchetti tramite lo smartphone (Booleano)
                                if "proxy_to_client" in mqtt:
                                    node_mqtt.proxy_to_client = bool(mqtt["proxy_to_client"])
                                    mqtt_updated = True

                                # 7. Abilitazione dell'output in formato JSON sulla rete (Booleano)
                                if "json_enabled" in mqtt:
                                    node_mqtt.json_enabled = bool(mqtt["json_enabled"])
                                    mqtt_updated = True

                                # 8. Cifratura dei pacchetti inviati al broker (Booleano)
                                if "encryption_enabled" in mqtt:
                                    node_mqtt.encryption_enabled = bool(mqtt["encryption_enabled"])
                                    mqtt_updated = True

                                # 9. Abilitazione connessione sicura TLS (Booleano)
                                if "tls_enabled" in mqtt:
                                    node_mqtt.tls_enabled = bool(mqtt["tls_enabled"])
                                    mqtt_updated = True
                                    
                            # ==========================================
                            # ESECUZIONE E COMMIT DELLA CONFIGURAZIONE
                            # ==========================================
                            # Scrittura dei moduli non distruttivi (senza riavvio)
                            if device_updated:
                                iface_global.localNode.writeConfig("device")
                                print("Configurazione 'device' scritta.")
                            if position_updated:
                                iface_global.localNode.writeConfig("position")
                                print("Configurazione 'position' scritta.")
                            if display_updated:
                                iface_global.localNode.writeConfig("display")
                                print("Configurazione 'display' scritta.")
                            if power_updated:
                                iface_global.localNode.writeConfig("power")
                                print("Configurazione 'power' scritta.")
                            if mqtt_updated:
                                iface_global.localNode.writeConfig("mqtt")
                                print("Configurazione 'mqtt' scritta con successo.")

                            # Gestione dei moduli distruttivi (che forzano il reboot hardware)
                            if lora_updated or network_updated or bluetooth_updated or security_updated:
                                if lora_updated:
                                    trigger_module = "lora"
                                elif network_updated:
                                    trigger_module = "network"
                                elif bluetooth_updated:
                                    trigger_module = "bluetooth"
                                else:
                                    trigger_module = "security"
                                print(f"Scrittura configurazione critica ({trigger_module}). Innesco reboot...")
                                
                                await websocket.send_text(json.dumps({
                                    "type": "node_rebooting",
                                    "message": "Il nodo richiede il riavvio hardware per applicare le modifiche LoRa/Network..."
                                }))
                                
                                try:
                                    # Invia il comando specifico che fa riavviare la MCU
                                    iface_global.localNode.writeConfig(trigger_module)
                                    await asyncio.sleep(0.2)
                                    iface_global.close()
                                except Exception:
                                    pass # Ignora la caduta immediata della seriale

                                port_path = iface_global.devPath
                                asyncio.create_task(handle_node_reconnect(port_path, websocket))
                            
                            else:
                                # Se abbiamo modificato solo moduli flash (device/position/display/power), il nodo NON si riavvia.
                                # Possiamo rispondere subito con l'ACK standard alla UI
                                await websocket.send_text(json.dumps({
                                    "type": "config_ack"
                                }))
                    
                        except Exception as e:
                            print("CONFIG SET ERROR:", e)
                            await websocket.send_text(json.dumps({
                                "type": "config_error",
                                "reason": str(e)
                            }))
                    
                        continue
                        
                except Exception as e:
                    print(f"[!] Errore invio mesh: {e}")

    except WebSocketDisconnect:
        manager.disconnect(websocket)
        
# --- INTERFACCIA WEB COMPLETA ---
html_content = """
<!DOCTYPE html>
<html>
<head>
    <link rel="icon" type="image/x-icon" href="/favicon.ico?v=1">
    <title>Meshtastic Client by IU0CRY</title>
    <style>
        body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 0; 
        background: #1e1e24; color: #fff; overflow: hidden; }
        .navbar { background: #111116; padding: 15px; font-size: 20px; font-weight: bold; 
        border-bottom: 2px solid #333; display: flex; justify-content: space-between; 
        align-items: center; }
        .container { display: flex; height: calc(100vh - 60px); }
        .sidebar-left { width: 360px; background: #15151a; border-right: 1px solid #333; 
        padding: 20px; box-sizing: border-box; display: flex; flex-direction: column; }
        .main-content { flex: 1; padding: 20px; display: flex; flex-direction: column; min-width: 0; }
        .sidebar-right { width: 360px; background: #18181c; border-left: 1px solid #333; padding: 20px; 
        box-sizing: border-box; overflow-y: auto; }
        .chat-box { flex: 1; background: #1a1a1f; border: 1px solid #333; border-radius: 8px; 
        padding: 10px; overflow-y: auto; display: flex; flex-direction: column; gap: 10px; }
        .chat-bubble { cursor: pointer; background: #2a2a35; padding: 10px; border-radius: 8px; 
        border-left: 4px solid #00bcd4; }
        .chat-meta { font-size: 11px; color: #888; margin-bottom: 4px; display: flex; 
        justify-content: space-between; }
        .chat-text { font-size: 13px; color: #fff; word-break: break-all; }
        .chat-input-container { display: flex; gap: 8px; margin-top: 10px; }
        .chat-input { flex: 1; background: #25252b; border: 1px solid #444; border-radius: 6px; 
        padding: 10px; color: #fff; font-size: 14px; }
        .chat-input:focus { border-color: #00bcd4; outline: none; }
        .chat-btn { background: #00bcd4; border: none; color: #fff; padding: 0 15px; 
        border-radius: 6px; font-weight: bold; cursor: pointer; }
        .chat-btn:hover { background: #008ba3; }
        .console { flex: 1; background: #000; border-radius: 8px; padding: 15px; overflow-y: auto; 
        font-family: 'Courier New', monospace; color: #00ff00; border: 1px solid #333; 
        font-size: 12px; line-height: 1.4; }
        .log-row { border-bottom: 1px solid #111; padding: 2px 0; white-space: normal;  
        word-break: break-word; }
        .info-tag { color: #00bcd4; } .debug-tag { color: #00bcd4; } .warn-tag { color: #ff9800; }
        .node-card { background: #25252b; border: 1px solid #444; border-radius: 6px; padding: 10px; 
        margin-bottom: 10px; }
        .node-name { font-weight: bold; font-size: 13px; color: #fff; }
        .node-id { font-family: monospace; color: #00bcd4; font-size: 11px; margin-top: 2px; }
        .status-badge { display: inline-block; padding: 5px 10px; border-radius: 4px; font-size: 12px; 
        font-weight: bold; }
        .online { background: #2e7d32; } .offline { background: #c62828; }
        .chat-bubble:hover { background: #3a3a48; }
        .node-card { cursor: pointer; transition: background 0.2s ease; }
        .node-card:hover { background: #3a3a48; }
    </style>
</head>
<body>
<div class="navbar" style="display: flex; align-items: center; width: 100%; box-sizing: border-box;">
        <div style="display: flex; align-items: stretch; gap: 10px;">
            <span style="display: flex; align-items: center;">📡 Meshtastic Client</span>        
            <div style="display: flex; align-items: center; border: 1px solid #ffffff; 
                padding: 5px 15px; border-radius: 6px; background-color: #000000; color: #ffffff;">
                <button class="chat-btn" onclick="cancellaCronologia()">❌ Elimina</button>
            </div>      
            <div style="display: flex; align-items: center; gap: 15px; border: 1px solid #ffffff; 
                padding: 5px 15px; border-radius: 6px; background-color: #000000; color: #ffffff;">
                <button id="saveConfigBtn" class="chat-btn">💾 Salva Configurazione</button>        
                <div id="syncBadge">Synced</div>
            </div>
        </div>
        <div id="status" class="status-badge offline" style="margin-left: auto; display: flex; align-items: center;">Connessione...</div>
    </div>
    <div class="container">
        <div class="sidebar-left">
            <h3>💬 Messaggi Chat Mesh</h3>
            <div class="chat-box" id="chatBox">
                <div id="no-chat-msg" style="color: #666; font-size: 12px; 
                text-align: center; margin-top: 20px;">In attesa di messaggi...</div>
            </div>
            <div class="chat-input-container">
                <select id="destinationSelect" class="chat-select">
                    <option value="broadcast">🌐 Broadcast</option>
                </select>
            </div>
            <div class="chat-input-container">
                <input type="text" id="msgInput" class="chat-input" placeholder="Messaggio..." autocomplete="off">
                <button id="sendBtn" class="chat-btn">💬</button>
                <button id="checkSendBtn" class="chat-btn">🔍💬</button><br>
            </div>
        </div>
        <div class="main-content">
            <h3>⚙️ Configurazione</h3>
            <div class="console" id="configBox"></div>
        </div>
        <div class="main-content">
            <h3>⚙️ Log di Sistema / Traffico Radio</h3>
            <div class="console" id="consoleLog"></div>
        </div>
        <div class="sidebar-right">
            <h3>👥 Nodi Rilevati (<span id="node-count">0</span>)</h3>
            <div id="nodesContainer"></div>
        </div>
    </div>
    <script>
let ws;
let nodoSelezionato = null;

const consoleLog = document.getElementById('consoleLog');
const chatBox = document.getElementById('chatBox');
const msgInput = document.getElementById('msgInput');
const sendBtn = document.getElementById('sendBtn');
const checkSendBtn = document.getElementById('checkSendBtn');
const nodesContainer = document.getElementById('nodesContainer');
const nodeCountLabel = document.getElementById('node-count');
const statusBadge = document.getElementById('status');

const dizionarioNodi = new Map();

// -------------------------
// SELEZIONE NODO
// -------------------------
function selezionaNodo(nodo) {

    nodoSelezionato = nodo;

    const sel = document.getElementById("destinationSelect");

    const nodeId = nodo.id || nodo;
    const nodeName = nodo.nome || nodo.name || nodeId;

    const value = `node:${nodeId}`;

    // 🔥 1. se non esiste la option la creo
    let option = [...sel.options].find(o => o.value === value);

    if (!option) {
        option = document.createElement("option");
        option.value = value;
        option.textContent = `👤 ${nodeName}`;
        sel.appendChild(option);
    }

    // 🔥 2. seleziono
    sel.value = value;
}

// -------------------------
// RENDER NODI
// -------------------------
function aggiornaNodiUI() {
    nodesContainer.innerHTML = "";

    const nodiArray = Array.from(dizionarioNodi.values());

    nodeCountLabel.textContent = nodiArray.length;

    nodiArray.forEach(nodo => {

        const radio = nodo.radio || {};
        const messages = Array.isArray(nodo.messages) ? nodo.messages : [];

        // -------------------------
        // NOME DISPLAY
        // -------------------------
        const displayName =
            nodo.longName ||
            nodo.nome ||
            nodo.shortName ||
            nodo.id_display ||
            nodo.id ||
            "Sconosciuto";
            
        const shortName =
            nodo.shortName ||
            "Sconosciuto";

        const isUnmess =
            nodo.user?.isUnmessagable ? "🚫 DM" : "📨 DM";
            
            //alert(nodo.isUnmessagable);

        // -------------------------
        // ID MESH (sempre !xxxx se possibile)
        // -------------------------
        const rawId =
            nodo.user?.id ||
            nodo.id ||
            nodo.id_display;

        const meshId =
            rawId;

        // -------------------------
        // ALTRI CAMPI
        // -------------------------
        const roleDisplay =
            nodo.role ||
            "unknown";

        const hwModel =
            nodo.model ||
            nodo.hwModel ||
            nodo.user?.hwModel ||
            "-";

        const licensed =
            nodo.isLicensed ??
            nodo.user?.isLicensed ??
            false;

        const lastMsg = messages.length > 0
            ? (messages[messages.length - 1].text || " ")
            : "Nessun messaggio";

        // -------------------------
        // CARD
        // -------------------------
        const card = document.createElement("div");
        card.className = "node-card";

        card.onclick = () => selezionaNodo(nodo);

        card.innerHTML = `            
            <div class="node-name">
                👤 ${shortName} - ${displayName} - ${isUnmess}
            </div>
            
            <div class="node-id" style="font-size:11px; color:#00bcd4;">
                🆔 ${meshId}
            </div>

            <div style="font-size:11px; color:#aaa;">
                🎭 ${roleDisplay}
            </div>

            <div style="font-size:11px; color:#aaa;">
                💻 ${hwModel}
            </div>

            <!-- opzionale chat preview -->
            <!--
            <div style="font-size:11px; color:#aaa;">
                🔑 ${licensed ? "Licensed" : "Unlicensed"}
            </div>
            -->

            <div style="margin-top:6px; font-size:11px; color:#aaa;">
                🧭 Hop: ${radio.lastHop ?? "-"} |
                📡 RSSI: ${radio.lastRssi ?? "-"}dBm |
                📶 SNR: ${radio.lastSnr ?? "-"} dB
            </div>

            <!-- opzionale chat preview -->
            <!--
            <div style="margin-top:8px; font-size:11px; color:#888;">
                💬 ${lastMsg}
            </div>
            -->
        `;

        nodesContainer.appendChild(card);
    });
}

// -------------------------
// CLEAN LOG
// -------------------------
function cleanLog(str) {
    return str
        .replace(/\x1b\[[0-9;]*m/g, '')
        .replace(/\s+/g, ' ')
        .trim();
}

// -------------------------
// FILL CHANNELS
// -------------------------
function popolaMenuDestinazioni(channels) {

    const sel = document.getElementById("destinationSelect");

    // pulisco la select
    sel.innerHTML = "";

    // 🌐 broadcast sempre presente
    sel.innerHTML += `<option value="broadcast">🌐 Broadcast</option>`;

    // 📻 canali dal backend
    channels.forEach(ch => {

        if (ch.index !== 0) {
            sel.innerHTML += `
                <option value="channel:${ch.index}">
                📻 ${ch.name}
            </option>
            `;
        }    
            
    });

    console.log("Canali caricati:", channels);
}

// -------------------------
// SEND MESSAGE
// -------------------------
sendBtn.onclick = () => {

    const msg = msgInput.value.trim();
    if (!msg || !ws) return;

    const dest = destinationSelect.value;
    
    const ora = new Date().toLocaleTimeString([], {
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit'
    });

    // 🔥 1. mostro subito in chat
    const msgEl = addMessage("a: " + dest + "<br>" + msg, "sending");
    
    ws.send(JSON.stringify({
        tipo: "chat",
        messaggio: msg,
        to: dest
    }));
    
    salvaMessaggio("Tu", dest, msg, ora);

    // salvo riferimento per update
    window.lastMsgEl = msgEl;

    msgInput.value = "";
};

// -------------------------
// CHECK SEND MESSAGE
// -------------------------
let messaggioOld = "Mario";

checkSendBtn.onclick = () => {

    const msg = msgInput.value.trim();
    if (!msg || !ws) return;
    
    messaggioOld = msg;
    
    const dest = destinationSelect.value;
    
    const payload = {
        tipo: "chat",
        messaggio: msg,
        to: dest
    };

    const msgEl = addMessage("🔍 Verifica traceroute <br>a: " + dest, "sending");

    ws.send(JSON.stringify({
        tipo: "traceroute",
        to: dest,
        message: msg
    }));

    // salvo riferimento per update
    window.lastMsgEl = msgEl;

    msgInput.value = "";
};

// -------------------------
// SAVE CONFIG BUTTON
// -------------------------
document.getElementById("saveConfigBtn").onclick = () => {
    ConfigManager.commit(ws);
};

// -------------------------
// WRITE CHAT MESSAGE AREA
// -------------------------
function addMessage(text, status = "sending") {

    const noChat = document.getElementById('no-chat-msg');
    if (noChat) noChat.remove();

    const ora = new Date().toLocaleTimeString([], {
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit'
    });

    const bubble = document.createElement('div');
    bubble.className = 'chat-bubble';

    bubble.innerHTML = `
        <div class="chat-meta">
            <span style="color:#4caf50;font-weight:bold;">
                Tu
            </span>
            <span>${ora}</span>
        </div>
        <div class="chat-text">
            ${text}
        </div>
    `;

    chatBox.appendChild(bubble);
    chatBox.scrollTop = chatBox.scrollHeight;

    return bubble;
}

// -------------------------
// WRITE CHAT MESSAGE AREA FROM SAVE
// -------------------------
function addMessageBack(text, status = "sending") {

    const noChat = document.getElementById('no-chat-msg');
    if (noChat) noChat.remove();

    const bubble = document.createElement('div');
    bubble.className = 'chat-bubble';

    bubble.innerHTML = `
        <div class="chat-meta">
            <span style="color:#4caf50;font-weight:bold;">
            </span>
        </div>
        <div class="chat-text">
            ${text}
        </div>
    `;

    // La regex cerca il simbolo ! seguito da qualsiasi carattere per 8 volte
    const risultato = text.match(/!.{0,8}/);
    
    if (risultato) {
        bubble.onclick = () => {
            console.log("CLICK CHAT", risultato);
            selezionaNodo({
                id: risultato,
                nome: risultato
            });
        };
    }
    
    chatBox.appendChild(bubble);
    chatBox.scrollTop = chatBox.scrollHeight;

    return bubble;
}

// -------------------------
// NORMALIZE NODE ID
// -------------------------
function normalizeNodeId(data) {

    // 1. PRIORITÀ ASSOLUTA: Meshtastic ID
    if (data.node?.user?.id) return data.node.user.id;

    // 2. NODEINFO diretto
    if (data.node?.id && String(data.node.id).startsWith("!")) {
        return data.node.id;
    }

    // 3. from_display (se backend lo manda già corretto)
    if (data.from_display && data.from_display.startsWith("!")) {
        return data.from_display;
    }

    // 4. fallback numerico → convertito in stringa
    if (data.from) return String(data.from);

    return null;
}

// -------------------------
// SAVE NODES
// -------------------------
function salvaNodi() {

    const obj = {};

    dizionarioNodi.forEach((nodo, id) => {

        obj[id] = {
            id: nodo.id,
            longName: nodo.longName,
            shortName: nodo.shortName,
            role: nodo.role,
            hwModel: nodo.hwModel,
            user: nodo.user,
            radio: nodo.radio,
            messages: nodo.messages
        };

    });

    localStorage.setItem("meshtastic_nodes", JSON.stringify(obj));
}

// -------------------------
// LOAD NODES
// -------------------------
function caricaNodi() {
    try {
        const dati = localStorage.getItem("meshtastic_nodes");
        if (!dati) return;

        const obj = JSON.parse(dati);

        dizionarioNodi.clear();

        Object.entries(obj).forEach(([id, nodo]) => {
            dizionarioNodi.set(id, {
                id,
                messages: nodo.messages || [],
                radio: nodo.radio || {},
                ...nodo
            });
        });

        console.log("Nodi caricati:", dizionarioNodi.size);

    } catch (e) {
        console.error("Errore load nodi", e);
    }
}

// -------------------------
// CONFIG MANAGER
// -------------------------
const ConfigManager = {

    device: {},      // config reale dal device
    draft: {},       // modifiche UI
    status: "idle",  // idle | dirty | syncing | synced

    // 🔥 carica dal device
    load(config) {
    
        this.device = structuredClone(config);
        this.status = "synced";
    
        // 🔥 FLATTEN per UI
        this.draft = {
            ...config.localConfig,
            moduleConfig: config.moduleConfig
        };
    
        renderConfig();
        updateSyncBadge();
    },

    // ✏️ modifica UI
    set(path, value) {
        this._setPath(this.draft, path, value);
        this.status = "dirty";
        renderConfig();
        updateSyncBadge();
    },

    // 📡 invia al device
    commit(ws) {
        this.status = "syncing";
        updateSyncBadge();

        ws.send(JSON.stringify({
            type: "set_config",
            config: this.draft
        }));
    },

    // 🔄 richiesta refresh device
    refresh(ws) {
        ws.send(JSON.stringify({
            type: "get_config"
        }));
    },

    // 🧠 utility path setter 
    _setPath(obj, path, value) {
        const keys = path.split('.');
        let ref = obj;

        for (let i = 0; i < keys.length - 1; i++) {
            if (!ref[keys[i]]) ref[keys[i]] = {};
            ref = ref[keys[i]];
        }

        ref[keys[keys.length - 1]] = value;
    }
};

function renderConfig() {

    const el = document.getElementById("configBox");
    const c = ConfigManager.draft;

    el.innerHTML = "";

    function renderSection(title, obj, path = "") {

        const section = document.createElement("div");
        section.style.marginBottom = "20px";

        const h = document.createElement("h3");
        h.textContent = title;
        section.appendChild(h);

        Object.entries(obj).forEach(([key, value]) => {

            const fullPath = path ? `${path}.${key}` : key;

            const wrapper = document.createElement("div");
            wrapper.style.marginBottom = "8px";

            const label = document.createElement("div");
            label.innerHTML = `🔧 <b>${key}</b>`;
            label.style.fontSize = "13px";
            label.style.color = "#00bcd4";

            wrapper.appendChild(label);

            // -------------------------
            // BOOL
            // -------------------------
            if (typeof value === "boolean") {

                const input = document.createElement("input");
                input.type = "checkbox";
                input.checked = value;

                input.onchange = () => {
                    ConfigManager.set(fullPath, input.checked);
                };

                wrapper.appendChild(input);
            }

            // -------------------------
            // NUMBER
            // -------------------------
            else if (typeof value === "number") {

                const input = document.createElement("input");
                input.type = "number";
                input.value = value;

                input.oninput = () => {
                    ConfigManager.set(fullPath, Number(input.value));
                };

                wrapper.appendChild(input);
            }

            // -------------------------
            // STRING
            // -------------------------
            else if (typeof value === "string") {

                const input = document.createElement("input");
                input.type = "text";
                input.value = value;

                input.oninput = () => {
                    ConfigManager.set(fullPath, input.value);
                };

                wrapper.appendChild(input);
            }

            // -------------------------
            // OBJECT (RECURSIVE)
            // -------------------------
            else if (typeof value === "object" && value !== null) {

                renderSection(key.toUpperCase(), value, fullPath);
            }

            section.appendChild(wrapper);
        });

        el.appendChild(section);
    }

    // ROOT SECTIONS
    renderSection("DEVICE", c.device || {}, "device");
    renderSection("POSITION", c.position || {}, "position");
    renderSection("POWER", c.power || {}, "power");
    renderSection("NETWORK", c.network || {}, "network");
    renderSection("DISPLAY", c.display || {}, "display");
    renderSection("LORA", c.lora || {}, "lora");
    renderSection("BLUETOOTH", c.bluetooth || {}, "bluetooth");
    renderSection("SECURITY", c.security || {}, "security");

    // moduleConfig separato
    renderSection("MQTT", c.moduleConfig?.mqtt || {}, "moduleConfig.mqtt");
}

// -------------------------
// UPDATE SYNC BADGE
// -------------------------
function updateSyncBadge() {
    const el = document.getElementById("syncBadge");

    el.textContent = ConfigManager.status.toUpperCase(); // Rende il testo più leggibile nella UI

    // Rimuove l'animazione di pulsazione se era attiva da stati precedenti
    el.style.animation = "none";

    el.style.color =
        ConfigManager.status === "synced" ? "#4caf50" : // Verde
        ConfigManager.status === "dirty" ? "#ff9800" :  // Arancione
        ConfigManager.status === "syncing" ? "#03a9f4" : // Azzurro
        ConfigManager.status === "rebooting" ? "#9c27b0" : // Viola (o un blu elettrico)
        ConfigManager.status === "timeout" ? "#f44336" :  // Rosso
        "#888";                                          // Grigio di default

    // Ottimizzazione visiva: aggiunge un effetto lampeggiante durante il riavvio o il syncing
    if (ConfigManager.status === "rebooting" || ConfigManager.status === "syncing") {
        el.style.animation = "pulse 1.5s infinite";
    }
}

// -------------------------
// SAVE MESSAGE
// -------------------------
function salvaMessaggio(from, dest, msg, ora) {

    let messaggi = JSON.parse(
        localStorage.getItem("chat_messages") || "[]"
    );

    messaggi.push({
        mitt: from,
        dest: dest,
        msg: msg,
        ts: ora
    });

    localStorage.setItem(
        "chat_messages",
        JSON.stringify(messaggi)
    );
}

// -------------------------
// LOAD MESSAGE
// -------------------------
function caricaMessaggi() {

    const messaggi = JSON.parse(
        localStorage.getItem("chat_messages") || "[]"
    );


    messaggi.forEach(m => {
        addMessageBack(
            `<span style="color:#00bfff;">da:</span> ${m.mitt}<br>
             <span style="color:#ffcc00;">a:</span> ${m.dest}<br>
             <span style="color:#00ff88;">Msg:</span> ${m.msg}<br>
             <span style="color:#888;">alle:</span> ${m.ts}`,
            "sending"
        );
    });
}

// -------------------------
// PURGE MESSAGE
// -------------------------
function cancellaCronologia() {
    localStorage.removeItem("chat_messages");
    document.getElementById("chatBox").innerHTML = "";
}

// -------------------------
// WEBSOCKET
// -------------------------
function connettiWebSocket() {

    ws = new WebSocket(`ws://${location.host}/ws`);

    ws.onopen = () => {
        statusBadge.textContent = "Connesso";
        statusBadge.className = "status-badge online";
        console.log("WebSocket connesso");
    };

    ws.onclose = () => {
        statusBadge.textContent = "Disconnesso";
        statusBadge.className = "status-badge offline";
    };

    ws.onerror = (e) => {
        console.error("WebSocket error:", e);
    };

    ws.onmessage = (event) => {
        console.log("ROW:", event.data);
        let data;

        try {
            data = JSON.parse(event.data);
        } catch (e) {
            console.error("JSON non valido:", event.data);
            return;
        }

        console.log("WS DATA:", data);

        const ora = new Date().toLocaleTimeString([], {
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit'
        });

        // =====================================================
        // 📡 INIT (CANALI / SETUP)
        // =====================================================
        if (data.type === "init") {
            popolaMenuDestinazioni(data.channels);
            return;
        }

        // =====================================================
        // 🔍 TRACEROUTE / CHECK SEND
        // =====================================================
        if (data.type === "trace_result") {

            const sel = document.getElementById("destinationSelect");
            const dest = sel.value;
            
            const con = document.getElementById("msgInput");
            const msg = con.value;
        
            if (data.ok) {
                const msgEl = addMessage("🟢 Percorso verificato <br>a: " + dest + "<br>" + messaggioOld, "sending");
                msgInput.value = "";
            } else {
                msgInput.value = "";
                const msgEl = addMessage("🔴 Destinazione non raggiungibile<br>" + dest, "error");
            }

            return;
        }

        // =====================================================
        // 💬 CHAT MESSAGE
        // =====================================================
        if (data.type === "chat") {
        
            console.log("PACKET CHAT:", data);
        
            // -------------------------
            // NORMALIZZAZIONE CAMPI
            // -------------------------
            const from = data.da || data.from || data.fromId || "unknown";
        
            const message =
                data.messaggio ||
                data.text ||
                (data.decoded ? data.decoded.text : "") ||
                "";
        
            const radio = data.radio || {};
            const rssi = radio.lastRssi ?? data.rxRssi ?? null;
            const snr = radio.lastSnr ?? data.rxSnr ?? null;
            const hops = radio.lastHop ?? data.hopLimit ?? null;
        
            const ora = new Date().toLocaleTimeString();
        
        
            salvaMessaggio(from, "Tu", message, ora);

            // -------------------------
            // UI CHAT GLOBALE
            // -------------------------
            const noChat = document.getElementById('no-chat-msg');
            if (noChat) noChat.remove();
        
            const bubble = document.createElement('div');
            bubble.className = 'chat-bubble';
        
            // -------------------------
            // COLORE QUALITÀ SEGNALE
            // -------------------------
            function getColor(snr) {
                if (snr == null) return "#666";
                if (snr >= 10) return "#00e676";
                if (snr >= 5) return "#cddc39";
                if (snr >= 0) return "#ff9800";
                return "#f44336";
            }

            const risultato = from.match(/!.{0,8}/);

            if (risultato) {
                bubble.onclick = () => {
                    console.log("CLICK CHAT", risultato);
                    selezionaNodo({
                        id: risultato,
                        nome: risultato
                    });
                };
            }
        
            bubble.style.borderLeft = `4px solid ${getColor(snr)}`;
                  
            // -------------------------
            // CONTENUTO UI
            // -------------------------
            bubble.innerHTML = `         
                <div class="chat-meta">
                    <span style="color:#00bcd4;font-weight:bold;">
                        ${from}
                    </span>
                    <span>${ora}</span>
                </div>
        
                <div class="chat-text">
                    ${message}
                </div>
                
                <!-- Allineiamo i dati radio e il pulsante sulla stessa riga visiva -->
                <div style="margin-top:6px; font-size:11px; color:#aaa; display: flex; 
                justify-content: space-between; align-items: center;">
                    <div>
                        🧭 Hop: ${hops ?? "-"} |
                        📡 RSSI: ${rssi ?? "-"} dBm |
                        📶 SNR: ${snr ?? "-"} dB
                    </div>
                </div>
            `;
            
            chatBox.appendChild(bubble);
            chatBox.scrollTop = chatBox.scrollHeight;

            return;
        }
        
        // =====================================================
        // 🧑 NODE INFO UPDATE
        // =====================================================
        if (data.type === "nodeinfo") {
        
            console.log("NODEINFO (!)):", data);
        
            const nodeId = normalizeNodeId(data);
            if (!nodeId) return;
        
            let nodo = dizionarioNodi.get(nodeId);
        
            if (!nodo) {
                nodo = {
                    id: nodeId,
                    messages: [],
                    radio: {}
                };
                dizionarioNodi.set(nodeId, nodo);
            }
        
            // aggiorna info utente
            if (data.node) {
                nodo.longName = data.node.longName || nodo.longName;
                nodo.shortName = data.node.shortName || nodo.shortName;
                nodo.role = data.node.role || nodo.role;
                nodo.hwModel = data.node.model || nodo.hwModel;
                nodo.user = data.node.user || nodo.user;
                nodo.id_display = data.node.id_display || nodeId;
            }
        
            // radio
            if (data.node?.radio) {
                nodo.radio = {
                    ...nodo.radio,
                    ...data.node.radio
                };
            }
        
            aggiornaNodiUI();
            console.log("SALVATAGGIO NODI:", dizionarioNodi);
            salvaNodi();

            return;
        }

        // =====================================================
        // 📡 GENERIC PACKET (TELEMETRY / POSITION / DEBUG)
        // =====================================================
/* 
        if (data.type === "radio") {
        
            const nodeId = normalizeNodeId(data);
            //const nodeId = normalizeNodeId(data.from || data.node?.id);

            if (!nodeId) return;
    
            const nodo = upsertNodo(nodeId, data);
        
            nodo.radio.lastRssi = data.radio?.lastRssi ?? data.radio?.rssi;
            nodo.radio.lastSnr = data.radio?.lastSnr ?? data.radio?.snr;
            nodo.radio.lastHop = data.radio?.lastHop ?? data.radio?.hops;
            
            console.log("NODE DEBUG 2:", {
                type: data.type,
                from: data.from,
                from_display: data.from_display,
                node_id: data.node?.id,
                user_id: data.node?.user?.id
            });
        
            aggiornaNodiUI();
            return;
        }
 */

        if (data.type === "config") {
            ConfigManager.load(data.config);
            return;
        }
        
        if (data.type === "config_ack") {
            ConfigManager.refresh(ws);
            return;
        }
        
        if (data.type === "node_rebooting") {
            ConfigManager.status = "rebooting";
            updateSyncBadge();
            UIManager.showLoadingOverlay("Il nodo si sta riavviando...");
            UIManager.disableControls(); 
            return;
        }

        if (data.type === "node_ready") {
            ConfigManager.status = "synced"; 
            updateSyncBadge();
            UIManager.hideLoadingOverlay();
            UIManager.enableControls();
            ConfigManager.refresh(ws); 
            return;
        }

        if (data.type === "node_timeout") {
            ConfigManager.status = "timeout";
            updateSyncBadge();
            UIManager.hideLoadingOverlay();
            UIManager.enableControls();
            return;
        }
        
        // =====================================================
        // 🪵 LOG FALLBACK (solo debug)
        // =====================================================
        const raw = JSON.stringify(data);

        const riga = document.createElement('div');
        riga.className = "log-row";

        riga.innerHTML = `
            <span>[${ora}]</span> <span class="debug-tag">${raw}</span>
        `;

        consoleLog.appendChild(riga);
        consoleLog.scrollTop = consoleLog.scrollHeight;
    };
}

// -------------------------
// START
// -------------------------
caricaNodi();
caricaMessaggi();
aggiornaNodiUI();
connettiWebSocket();    
</script>
"""

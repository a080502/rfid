import serial
import time
import sys
import csv
from datetime import datetime
import threading
import binascii
import struct
import collections # Importato per deque
import mysql.connector # Importato per la connessione a MySQL
from mysql.connector import Error

# --- Configurazione della porta seriale RFID ---
PORTA_COM_RFID = 'COM6'
BAUD_RATE_RFID = 115200
BYTE_SIZE_RFID = serial.EIGHTBITS
PARITY_RFID = serial.PARITY_NONE
STOP_BITS_RFID = serial.STOPBITS_ONE
TIMEOUT_CONNESSIONE_RFID = 1

# --- Configurazione MySQL Database ---
DB_HOST = 'localhost'  # Indirizzo del server MySQL
DB_NAME = 'rfid'       # Nome del database
DB_USER = 'root'       # Nome utente
DB_PASS = '080502'     # Password
DB_TABLE = 'lettore'   # Nome della tabella

# --- Configurazione per il file CSV ---
NOME_FILE_CSV = 'rfid_tags_rssi.csv' # Nome file modificato

# --- Configurazione della velocità e Logica RSSI ---
INTERVALLO_TRA_LETTURE = 0.05 # Secondi tra la fine di un ciclo di lettura/processamento e l'inizio del successivo
ATTESA_RISPOSTA = 0.15  # Tempo di attesa per raccogliere le risposte del lettore in un ciclo
MAX_TAG_RECENTI = 20    # Numero massimo di tag unici da tenere in memoria per evitare registrazioni ripetute immediate

# --- Stringhe esadecimali da inviare ---
hex_strings_da_inviare = [
    'BB 01 03 00 10 00 4D 31 30 30 20 32 36 64 42 6D 20 56 31 2E 30 92 7E', # Seq 1 (Inizializzazione)
    'BB 01 03 00 07 01 56 32 2E 33 2E 31 54 7E', # Seq 2 (Inizializzazione)
    'BB 01 08 00 01 03 0D 7E', # Seq 3 (Inizializzazione)
    'BB 01 FF 00 01 15 16 7E', # Seq 4 (Non usata, risposta di errore esempio)
    'BB 00 22 00 00 22 7E'  # COMANDO DI LETTURA TAG SINGOLA (Indice 4)
]

# --- Converti le stringhe esadecimali in oggetti bytes ---
sequenze_bytes = []
for hex_str in hex_strings_da_inviare:
    try:
        cleaned_hex = hex_str.replace(" ", "")
        dati_bytes = bytes.fromhex(cleaned_hex)
        sequenze_bytes.append(dati_bytes)
    except ValueError as e:
        print(f"ERRORE: La stringa esadecimale '{hex_str}' non è valida. Dettaglio: {e}")
        sys.exit(1)

# Verifica che ci siano abbastanza sequenze
if len(sequenze_bytes) < 5:
    print("ERRORE: Sequenze insufficienti definite.")
    sys.exit(1)

# --- Funzioni di Utilità ---
def hex_byte_to_signed_int(hex_byte):
    """Converte un byte esadecimale (0-255) in un intero con segno (-128 a 127)."""
    value = int(hex_byte, 16)
    if value > 127:
        value -= 256
    return value

def estrai_dati_tag(dati_bytes):
    """
    Estrae RSSI, PC, EPC e crea la stringa combinata dai dati grezzi ricevuti.
    Restituisce (rssi_int, pc_hex, epc_hex, stringa_combinata, valori_estratti, tag_hex_completo) o None se i dati non sono validi.
    """
    try:
        # Converti bytes in stringa hex per controllare il formato
        tag_hex_completo = dati_bytes.hex(' ').upper()
        
        # Verifica se il pacchetto inizia con BB 02 22 00 11 e termina con 7E
        if not (tag_hex_completo.startswith("BB 02 22 00 11") and tag_hex_completo.endswith("7E")):
            return None
            
        # Verifica lunghezza minima per contenere Header, Type, Cmd, PL, RSSI, PC (2 byte), almeno 1 byte EPC
        if len(dati_bytes) < 10:
             return None

        # Estrai RSSI (byte all'indice 5)
        rssi_hex = dati_bytes[5:6].hex().upper()
        rssi_int = hex_byte_to_signed_int(rssi_hex)

        # Estrai PC (2 bytes agli indici 6 e 7)
        pc_hex = dati_bytes[6:8].hex().upper()

        # Calcola lunghezza EPC in bytes basata su PC
        pc_val = int(pc_hex, 16)
        epc_word_length = (pc_val >> 11) & 0x1F
        epc_byte_length = epc_word_length * 2

        # Verifica se la lunghezza dei dati è sufficiente
        expected_min_len = 8 + epc_byte_length + 2 + 1

        # Estrai EPC (a partire dall'indice 8)
        epc_bytes = dati_bytes[8 : 8 + epc_byte_length]
        epc_hex = epc_bytes.hex().upper()

        # Estrai i valori per il CSV
        coppie_hex_completo = tag_hex_completo.split()
        if len(coppie_hex_completo) >= 8 + epc_byte_length:
             valori_estratti_csv = coppie_hex_completo[8:min(20, 8 + epc_byte_length)]
             while len(valori_estratti_csv) < 12:
                 valori_estratti_csv.append('')
        else:
             valori_estratti_csv = [''] * 12

        # Usa l'EPC completo come identificatore univoco
        stringa_combinata = epc_hex

        return rssi_int, pc_hex, epc_hex, stringa_combinata, valori_estratti_csv, tag_hex_completo

    except (IndexError, ValueError) as e:
        print(f"ERRORE nell'estrazione dati dal pacchetto: {dati_bytes.hex(' ').upper()} - {e}")
        return None

# --- Classe MySQLConnector per la connessione e invio a MySQL ---
class MySQLConnector:
    def __init__(self, host, database, user, password, table):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.table = table
        self.connection = None
        self.connected = False
        self.passage_counter = 1
        self.tag_hits = {}
        self.lock = threading.Lock()
        self.file_number = ""  # Nuovo: memorizza il numero del file
    
    def connect(self):
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password
            )
            if self.connection.is_connected():
                self.connected = True
                print(f"Connessione a MySQL riuscita. Database: {self.database}, Tabella: {self.table}")
                return True
            return False
        except Error as e:
            print(f"ERRORE nella connessione a MySQL: {e}")
            return False
    
    def disconnect(self):
        if self.connection and self.connection.is_connected():
            self.connection.close()
            self.connected = False
            print("Disconnessione da MySQL completata.")

    def get_last_passage(self):
        """Recupera l'ultimo numero di passage dal database"""
        if not self.reconnect_if_needed():
            return 1
        
        try:
            cursor = self.connection.cursor()
            query = f"SELECT MAX(passage) FROM {self.table}"
            cursor.execute(query)
            result = cursor.fetchone()
            cursor.close()
            
            if result[0] is not None:
                return result[0] + 1
            return 1
        except Error as e:
            print(f"ERRORE nel recupero dell'ultimo passage: {e}")
            return 1
    
    def reconnect_if_needed(self):
        if not self.connected or not self.connection or not self.connection.is_connected():
            print("Tentativo di riconnessione a MySQL...")
            return self.connect()
        return True
    
    def record_tag(self, tag_id, timestamp, rssi_int):
        if not self.reconnect_if_needed():
            print("ERRORE: Impossibile registrare il tag nel database - connessione non disponibile.")
            return False
        
        try:
            with self.lock:
                tag_id_shortened = tag_id[-8:] if len(tag_id) >= 8 else tag_id
                
                if tag_id_shortened in self.tag_hits:
                    self.tag_hits[tag_id_shortened] += 1
                else:
                    self.tag_hits[tag_id_shortened] = 1
                
                date_str = timestamp.strftime('%Y-%m-%d')
                time_str = timestamp.strftime('%H:%M:%S.%f')[:-3]
                unix_timestamp = int(timestamp.timestamp())
                rssi_formatted = f"{rssi_int} dBm"
                
                cursor = self.connection.cursor()
                
                insert_query = f"""
                INSERT INTO {self.table} 
                (passage, chip, date, time, `loop`, hits, timestamp, file, rssi, libero_2, libero_3) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                values = (
                    self.passage_counter,
                    tag_id_shortened,
                    date_str,
                    time_str,
                    1,
                    self.tag_hits[tag_id_shortened],
                    unix_timestamp,
                    self.file_number,  # Ora usa file_number nel campo libero
                    rssi_formatted,
                    "",
                    ""
                )
                
                cursor.execute(insert_query, values)
                self.connection.commit()
                self.passage_counter += 1
                cursor.close()
                
                print(f"-> MySQL: Tag={tag_id_shortened} (da {tag_id}), RSSI={rssi_formatted}, Time={time_str}, Passage={self.passage_counter-1}, Hits={self.tag_hits[tag_id_shortened]}, File={self.file_number}")
                return True
                
        except Error as e:
            print(f"ERRORE nell'inserimento dati MySQL: {e}")
            self.connected = False
            return False

# --- Thread per la scrittura su file CSV ---
class CsvWriterThread(threading.Thread):
    def __init__(self, nome_file):
        threading.Thread.__init__(self, daemon=True)
        self.nome_file = nome_file
        self.data_queue = collections.deque()
        self.lock = threading.Lock()
        self.running = True
        self.event = threading.Event()

    def add_data(self, data):
        with self.lock:
            self.data_queue.append(data)
        self.event.set()

    def run(self):
        while self.running or len(self.data_queue) > 0:
            self.event.wait(timeout=0.5)
            self.event.clear()

            data_to_write = []
            with self.lock:
                while self.data_queue:
                     data_to_write.append(self.data_queue.popleft())

            if data_to_write:
                try:
                    with open(self.nome_file, 'a', newline='', encoding='utf-8') as csvfile:
                        writer = csv.writer(csvfile)
                        csvfile.seek(0, 2)
                        if csvfile.tell() == 0:
                             writer.writerow(['Data', 'Ora', 'RSSI (dBm)',
                                              'Coppia_9', 'Coppia_10', 'Coppia_11', 'Coppia_12',
                                              'Coppia_13','Coppia_14','Coppia_15','Coppia_16',
                                              'Coppia_17','Coppia_18','Coppia_19','Coppia_20',
                                              'EPC_ID', 'Tag_Completo_Hex'])
                        writer.writerows(data_to_write)
                except IOError as e:
                    print(f"ERRORE nella scrittura CSV su '{self.nome_file}': {e}")
                except Exception as e:
                    print(f"Errore generico nella scrittura CSV: {e}")

            if not self.running and not self.data_queue:
                 break

    def stop(self):
        print("CSV Writer: Segnale di stop ricevuto...")
        self.running = False
        self.event.set()

# --- Connessione, invio e ascolto ---
ser_rfid = None
csv_writer = None
mysql_connector = None

try:
    # --- Inizializzazione CSV Writer ---
    csv_writer = CsvWriterThread(NOME_FILE_CSV)
    csv_writer.start()
    print(f"CSV Writer avviato per il file '{NOME_FILE_CSV}'.")

    # --- Richiesta numero file ---
    file_number = input("Inserisci il numero del file: ").strip()
    
    # --- Inizializzazione connessione MySQL ---
    mysql_connector = MySQLConnector(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        table=DB_TABLE
    )
    
    mysql_connected = mysql_connector.connect()
    if not mysql_connected:
        print("ATTENZIONE: Connessione a MySQL non riuscita. I dati non verranno inviati al database.")
    else:
        # Chiedi se azzerare il contatore passage
        last_passage = mysql_connector.get_last_passage()
        reset_choice = input(f"Ultimo passage trovato: {last_passage-1}. Vuoi azzerare il contatore? (s/n): ").strip().lower()
        
        if reset_choice == 's':
            mysql_connector.passage_counter = 1
            print("Contatore passage azzerato.")
        else:
            mysql_connector.passage_counter = last_passage
            print(f"Contatore passage continuerà da {last_passage}")
        
        # Imposta il numero del file
        mysql_connector.file_number = file_number
        print(f"Numero file impostato: {file_number}")

    # Scrittura intestazione iniziale CSV (se il file non esiste o è vuoto)
    try:
        with open(NOME_FILE_CSV, 'a', newline='', encoding='utf-8') as f:
             if f.tell() == 0:
                  writer = csv.writer(f)
                  writer.writerow(['Data', 'Ora', 'RSSI (dBm)',
                                   'Coppia_9', 'Coppia_10', 'Coppia_11', 'Coppia_12',
                                   'Coppia_13','Coppia_14','Coppia_15','Coppia_16',
                                   'Coppia_17','Coppia_18','Coppia_19','Coppia_20',
                                   'EPC_ID', 'Tag_Completo_Hex'])
    except IOError as e:
         print(f"ERRORE: Impossibile scrivere l'intestazione CSV iniziale: {e}")

    # --- Connessione al lettore RFID ---
    print(f"Connessione al lettore RFID su {PORTA_COM_RFID}...")
    ser_rfid = serial.Serial(
        port=PORTA_COM_RFID,
        baudrate=BAUD_RATE_RFID,
        bytesize=BYTE_SIZE_RFID,
        parity=PARITY_RFID,
        stopbits=STOP_BITS_RFID,
        timeout=TIMEOUT_CONNESSIONE_RFID
    )

    if ser_rfid.is_open:
        print("Connessione al lettore RFID riuscita.")

        # --- Inizializzazione lettore RFID ---
        print("Inizializzazione lettore RFID...")
        ser_rfid.reset_input_buffer()
        ser_rfid.reset_output_buffer()
        time.sleep(0.1)

        for i in range(3):
            if i < len(sequenze_bytes):
                 print(f"Invio init seq {i+1}: {sequenze_bytes[i].hex(' ').upper()}")
                 ser_rfid.write(sequenze_bytes[i])
                 time.sleep(0.2)
                 if ser_rfid.in_waiting > 0:
                      risposta_init = ser_rfid.read(ser_rfid.in_waiting)
                      print(f"Risposta init {i+1}: {risposta_init.hex(' ').upper()}")
            else:
                 print(f"ATTENZIONE: Sequenza di inizializzazione {i+1} non trovata.")
        print("Inizializzazione completata.")
        time.sleep(0.5)

        print(f"Avvio lettura tag RFID. Intervallo ciclo: {INTERVALLO_TRA_LETTURE + ATTESA_RISPOSTA:.2f} sec. Premi Ctrl+C per interrompere.")
        print("---------------------------------------------------")

        # Comando di lettura tag singola (Indice 4)
        read_command_sequence = sequenze_bytes[4]

        # Memorizza tag recenti per evitare registrazioni duplicate immediate
        tag_recenti_registrati = collections.deque(maxlen=MAX_TAG_RECENTI)

        # --- Ciclo Principale di Lettura e Processamento ---
        while True:
            letture_nel_ciclo = []

            # 1. Pulisci buffer e invia il comando di lettura
            ser_rfid.reset_input_buffer()
            ser_rfid.write(read_command_sequence)

            # 2. Attendi e raccogli le risposte
            tempo_inizio_attesa = time.monotonic()
            while time.monotonic() - tempo_inizio_attesa < ATTESA_RISPOSTA:
                if ser_rfid.in_waiting > 0:
                    dati_ricevuti_raw = ser_rfid.read(ser_rfid.in_waiting)

                    dati_tag = estrai_dati_tag(dati_ricevuti_raw)

                    if dati_tag:
                        rssi, pc, epc, epc_combinato, valori_csv, tag_hex_comp = dati_tag
                        ora_lettura = datetime.now()
                        letture_nel_ciclo.append({
                            "timestamp": ora_lettura,
                            "epc": epc_combinato,
                            "rssi": rssi,
                            "valori_csv": valori_csv,
                            "tag_hex": tag_hex_comp
                        })
                time.sleep(0.01)

            # 3. Processa le letture raccolte nel ciclo
            if letture_nel_ciclo:
                miglior_lettura = max(letture_nel_ciclo, key=lambda x: x["rssi"])

                epc_vincente = miglior_lettura["epc"]
                rssi_vincente = miglior_lettura["rssi"]
                timestamp_vincente = miglior_lettura["timestamp"]
                valori_csv_vincente = miglior_lettura["valori_csv"]
                tag_hex_vincente = miglior_lettura["tag_hex"]

                epc_vincente_short = epc_vincente[-8:] if len(epc_vincente) >= 8 else epc_vincente

                if epc_vincente_short not in tag_recenti_registrati:
                    tag_recenti_registrati.append(epc_vincente_short)

                    data_str = timestamp_vincente.strftime('%Y-%m-%d')
                    ora_str = timestamp_vincente.strftime('%H:%M:%S.%f')[:-3]

                    print(f"*** Vincitore Rilevato: EPC={epc_vincente_short} (da {epc_vincente}) | RSSI={rssi_vincente} dBm | Ora={ora_str} ***")

                    dati_per_csv = [data_str, ora_str, rssi_vincente] + valori_csv_vincente + [epc_vincente, tag_hex_vincente]

                    if csv_writer and csv_writer.is_alive():
                        csv_writer.add_data(dati_per_csv)

                    if mysql_connector and mysql_connector.connected:
                        mysql_connector.record_tag(epc_vincente, timestamp_vincente, rssi_vincente)

            # 5. Attendi l'intervallo stabilito prima del prossimo ciclo
            time.sleep(INTERVALLO_TRA_LETTURE)

    else:
        print(f"ERRORE: Impossibile aprire la porta {PORTA_COM_RFID} per il lettore RFID.")

except serial.SerialException as e:
    print(f"ERRORE SERIALE: {e}")
    import traceback
    traceback.print_exc()

except KeyboardInterrupt:
    print("\nOperazione interrotta dall'utente.")

except Exception as e:
    print(f"ERRORE INASPETTATO: {e}")
    import traceback
    traceback.print_exc()

finally:
    print("Avvio procedura di chiusura...")
    if csv_writer and csv_writer.is_alive():
        print("Arresto CSV writer...")
        csv_writer.stop()
        csv_writer.join(timeout=2.0)
        if csv_writer.is_alive():
             print("Attenzione: CSV writer non si è fermato correttamente.")
        else:
             print("CSV writer arrestato.")

    if mysql_connector:
        print("Disconnessione da MySQL Database...")
        mysql_connector.disconnect()
        print("MySQL Database disconnesso.")

    if ser_rfid is not None and ser_rfid.is_open:
        print(f"Chiusura porta RFID {PORTA_COM_RFID}...")
        ser_rfid.close()
        print(f"Porta {PORTA_COM_RFID} del lettore RFID chiusa.")

    print("Script terminato.")
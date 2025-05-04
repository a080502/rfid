import socket
import time
import datetime
import threading
import mysql.connector
from mysql.connector import Error
import sys
from collections import defaultdict
import pytz

class TCPServer:
    def __init__(self, host='0.0.0.0', port=3601, db_config=None, user_login='Demonte', keep_alive_seconds=5, error_log_throttle=60):
        self.host = host
        self.port = port
        self.server_socket = None
        self.client_socket = None
        self.user_login = user_login
        self.rome_timezone = pytz.timezone('Europe/Rome')
        self.db_config = db_config or {
            'host': 'localhost',
            'user': 'root',
            'password': '080502',
            'database': 'rfid'
        }
        self.file_number = "1"  # Default value
        self.command_handlers = {
            'SETPROTOCOL': self.handle_setprotocol,
            'GETSTATUS': self.handle_getstatus,
            'GETCONFIG': self.handle_getconfig,
            'GETFIRMWAREVERSION': self.handle_getfirmwareversion,
            'PASSINGS': self.handle_passings,
            'SETPUSHPASSINGS': self.handle_setpushpassings,
            'STOPOPERATION': self.handle_stopoperation,
        }
        self.running = True
        self.db_connection = None
        self.cursor = None
        self.last_processed_id = 0
        self.refresh_enabled = False
        self.clients = []
        self.keep_alive_seconds = keep_alive_seconds
        self.error_log_throttle = error_log_throttle
        self.error_log_times = defaultdict(int)
        self.error_counters = defaultdict(int)
        self.log_level = "INFO"

    def set_file_number(self, number):
        self.file_number = str(number)
        self.log("INFO", f"Numero file impostato a: {self.file_number}")

    def log(self, level, message):
        log_levels = {"DEBUG": 0, "INFO": 1, "WARNING": 2, "ERROR": 3}
        
        if log_levels.get(level, 4) >= log_levels.get(self.log_level, 1):
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"[{timestamp}] [{level}] {message}")

    def log_data_sent(self, message):
        if not message.startswith("#KEEPALIVE"):
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"\n→ DATI INVIATI A RACE RESULT [{timestamp}]:")
            print(f"  {message.strip()}\n")

    def set_log_level(self, level):
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR"]
        if level.upper() in valid_levels:
            self.log_level = level.upper()
            self.log("INFO", f"Log level set to {self.log_level}")
        else:
            self.log("WARNING", f"Invalid log level '{level}'. Using current level: {self.log_level}")

    def throttled_error_log(self, error_key, message):
        if "WinError 10053" in message:
            return
            
        current_time = time.time()
        last_logged = self.error_log_times.get(error_key, 0)
        
        self.error_counters[error_key] += 1
        
        if current_time - last_logged > self.error_log_throttle:
            count = self.error_counters[error_key]
            if count > 1:
                self.log("ERROR", f"{message} (occurred {count} times)")
            else:
                self.log("ERROR", message)
                
            self.error_counters[error_key] = 0
            self.error_log_times[error_key] = current_time

    def connect_to_database(self):
        try:
            if self.db_connection and self.db_connection.is_connected():
                return True
                
            self.db_connection = mysql.connector.connect(**self.db_config)
            if self.db_connection.is_connected():
                self.cursor = self.db_connection.cursor(dictionary=True)
                self.log("INFO", f"Connesso al database MySQL: {self.db_config['database']}")
                return True
        except Error as e:
            self.throttled_error_log("db_connect", f"Errore durante la connessione al database MySQL: {e}")
            return False

    def disconnect_from_database(self):
        if self.cursor:
            self.cursor.close()
        if self.db_connection and self.db_connection.is_connected():
            self.db_connection.close()
            self.log("INFO", "Connessione al database MySQL chiusa")

    def get_current_rome_time(self):
        now = datetime.datetime.now(self.rome_timezone)
        return now.strftime('%Y-%m-%d'), now.strftime('%H:%M:%S')

    def get_new_records(self):
        try:
            self.db_connection.commit()
            
            query = f"""
                SELECT id, passage, chip, date, time, `loop`, hits, timestamp, file 
                FROM lettore 
                WHERE id > {self.last_processed_id} 
                AND file = '{self.file_number}'
                ORDER BY id
            """
            self.cursor.execute(query)
            records = self.cursor.fetchall()
            
            if records:
                self.last_processed_id = records[-1]['id']
                self.log("DEBUG", f"Trovati {len(records)} nuovi record per file {self.file_number}. Ultimo ID: {self.last_processed_id}")
            
            return records
        except Error as e:
            self.throttled_error_log("db_fetch", f"Errore durante il recupero dei nuovi dati: {e}")
            self.connect_to_database()
            return []

    def get_all_records(self):
        try:
            self.db_connection.commit()
            
            query = f"""
                SELECT id, passage, chip, date, time, `loop`, hits, timestamp, file 
                FROM lettore 
                WHERE file = '{self.file_number}'
                ORDER BY id
            """
            self.cursor.execute(query)
            records = self.cursor.fetchall()
            
            if records:
                self.last_processed_id = records[-1]['id']
            
            return records
        except Error as e:
            self.throttled_error_log("db_fetch_all", f"Errore durante il recupero di tutti i dati: {e}")
            self.connect_to_database()
            return []

    def handle_setprotocol(self, command):
        return 'SETPROTOCOL;2.0'

    def handle_getstatus(self, command):
        date_str, time_str = self.get_current_rome_time()
        return f'GETSTATUS;{date_str};{time_str};1;11111111;1;{self.file_number};1;46.067377,11.462447;1;69;45;;;;;;;2;;;0;13.23'

    def handle_getconfig(self, command):
        parts = command.split(';')
        if len(parts) < 3:
            return 'ERROR;Invalid command format'

        category = parts[1]
        parameter = parts[2]
    
        config_responses = {
            ('GENERAL', 'BOXNAME'): f'{self.user_login};{self.user_login}',
            ('UPLOAD', 'CUSTNO'): '12345',
            ('DETECTION', 'DEADTIME'): '0',
            ('DETECTION', 'REACTIONTIME'): '0',
            ('DETECTION', 'NOTIFICATION'): 'BEEP+BLINK'
        }

        if (category, parameter) in config_responses:
            response = f"GETCONFIG;{category};{parameter};{config_responses[(category, parameter)]}"
            self.log("DEBUG", f"Invio risposta: {response}")
            self.send_to_client(response + "\r\n")
            return None

        return 'ERROR;Unknown configuration'

    def handle_getfirmwareversion(self, command):
        return 'GETFIRMWAREVERSION;2.72'

    def handle_passings(self, command):
        try:
            if not self.db_connection or not self.db_connection.is_connected():
                if not self.connect_to_database():
                    self.log("ERROR", "Impossibile connettersi al database per PASSINGS")
                    return None
            
            self.db_connection.commit()
            
            query = f"""
                SELECT passage 
                FROM lettore 
                WHERE file = '{self.file_number}'
                ORDER BY id DESC 
                LIMIT 1
            """
            self.cursor.execute(query)
            result = self.cursor.fetchone()
            
            if result and 'passage' in result:
                response = f'PASSINGS;{result["passage"]}'
            else:
                response = 'PASSINGS;0'
                
            self.log("INFO", f"Invio risposta: {response}")
            self.send_to_client(response + "\r\n")
            
            new_records = self.get_new_records()
            
            if new_records:
                for record in new_records:
                    passage_data = f"#P;{record['passage']};{record['chip']};{record['date']};{record['time']};{record['hits']};18;88;2;A;0;{record['loop']};1678883122123;12850;41.2"
                    self.log_data_sent(passage_data)
                    self.send_to_client(passage_data + "\r\n")
                    time.sleep(0.1)
            else:
                self.log("DEBUG", f"Nessun nuovo record da inviare per file {self.file_number}")
            
            self.refresh_enabled = True
        
        except Error as e:
            self.throttled_error_log("passings_error", f"Errore durante il recupero o l'invio dei dati PASSINGS: {e}")
            self.connect_to_database()
        
        return None

    def handle_setpushpassings(self, command):
        parts = command.split(';')
        if len(parts) < 3:
            return 'ERROR;Invalid SETPUSHPASSINGS format'
        try:
            if parts[1] == '1' and parts[2] == '1':
                response = 'SETPUSHPASSINGS;1;1'
                self.log("INFO", f"Invio risposta: {response}")
                self.send_to_client(response + "\r\n")
                
                temp_last_id = self.last_processed_id
                self.last_processed_id = 0
                reader_data = self.get_all_records()
                
                if reader_data:
                    for record in reader_data:
                        passage_data = f"#P;{record['passage']};{record['chip']};{record['date']};{record['time']};{record['hits']};18;88;2;A;55;{record['loop']};1678883122123;12850;41.2"
                        self.log_data_sent(passage_data)
                        self.send_to_client(passage_data + "\r\n")
                        time.sleep(0.1)
                
                self.refresh_enabled = True
                return None
            elif parts[1] == '1' and parts[2] == '0':
                self.refresh_enabled = False
                return 'SETPUSHPASSINGS;1;0'
            else:
                return 'ERROR;Invalid parameters'
        except Exception as e:
            self.throttled_error_log("pushpassings_error", f"Errore nell'elaborazione di SETPUSHPASSINGS: {e}")
            return 'ERROR;SETPUSHPASSINGS processing error'

    def handle_stopoperation(self, command):
        self.refresh_enabled = False
        return 'STOPOPERATION;OK'

    def process_command(self, command):
        command = command.strip()
        if not command:
            return None

        self.log("INFO", f'Comando ricevuto: {command}')

        command_parts = command.split(';')
        base_command = command_parts[0]

        handler = self.command_handlers.get(base_command)
        if handler:
            response = handler(command)
            if response is None:
                return None
        else:
            response = 'ERROR;Comando sconosciuto'

        return response + '\r\n'

    def handle_client(self, client_socket, address):
        client_id = f"{address[0]}:{address[1]}"
        self.log("INFO", f"Nuova connessione da {client_id}")
        self.clients.append(client_socket)
        
        buffer = ""
        connection_errors = 0
        max_connection_errors = 5
        
        try:
            while self.running:
                try:
                    data = client_socket.recv(1024)
                    if not data:
                        time.sleep(0.5)
                        continue
                    
                    connection_errors = 0
                    
                    for char in data.decode('utf-8'):
                        if char == '\n':
                            if buffer:
                                response = self.process_command(buffer.strip())
                                if response:
                                    self.log_data_sent(response.strip())
                                    client_socket.sendall(response.encode('utf-8'))
                            buffer = ""
                        else:
                            buffer += char
                except ConnectionResetError as e:
                    connection_errors += 1
                    error_key = f"conn_reset_{client_id}"
                    
                    if connection_errors <= max_connection_errors:
                        self.throttled_error_log(error_key, f"Il client {client_id} ha resettato la connessione: {e}")
                    
                    time.sleep(1)
                    continue
                except socket.timeout:
                    continue
                except Exception as e:
                    connection_errors += 1
                    error_key = f"client_read_{client_id}_{type(e).__name__}"
                    
                    if connection_errors <= max_connection_errors:
                        self.throttled_error_log(error_key, f"Errore durante la lettura dal client {client_id}: {e}")
                    
                    time.sleep(1)
                    continue
        except Exception as e:
            self.throttled_error_log(f"client_critical_{client_id}", f"Errore critico nella gestione del client {client_id}: {e}")
        finally:
            if client_socket in self.clients:
                self.log("INFO", f"Disconnessione del client {client_id}")
                self.clients.remove(client_socket)
            client_socket.close()

    def send_to_client(self, message):
        if not self.clients:
            if not message.startswith("#KEEPALIVE"):
                self.log("DEBUG", f"Nessun client connesso. Messaggio in attesa: {message.strip()}")
            return
            
        clients_to_remove = []
        for client in self.clients:
            try:
                client.sendall(message.encode('utf-8'))
                if not message.startswith("#KEEPALIVE"):
                    self.log_data_sent(message.strip())
            except ConnectionResetError:
                clients_to_remove.append(client)
            except BrokenPipeError:
                clients_to_remove.append(client)
            except Exception as e:
                clients_to_remove.append(client)
                self.throttled_error_log("send_error", f"Errore nell'invio al client: {e}")
        
        for client in clients_to_remove:
            if client in self.clients:
                self.clients.remove(client)
                self.log("DEBUG", f"Client rimosso dalla lista. Client attivi: {len(self.clients)}")

    def periodic_refresh(self):
        keep_alive_counter = 0
        
        while self.running:
            try:
                keep_alive_counter += 1
                if keep_alive_counter >= self.keep_alive_seconds * 2:
                    if self.clients:
                        self.send_to_client("#KEEPALIVE\r\n")
                        self.log("DEBUG", "Invio keep-alive per mantenere la connessione attiva")
                    keep_alive_counter = 0
                
                if self.refresh_enabled and self.db_connection:
                    self.db_connection.ping(reconnect=True)
                    
                    new_records = self.get_new_records()
                    if new_records:
                        keep_alive_counter = 0
                        for record in new_records:
                            passage_data = f"#P;{record['passage']};{record['chip']};{record['date']};{record['time']};{record['hits']};18;88;2;A;55;{record['loop']};1678883122123;12850;41.2"
                            self.log_data_sent(passage_data)
                            self.send_to_client(passage_data + "\r\n")
                            time.sleep(0.1)
            except Exception as e:
                self.throttled_error_log("refresh_error", f"Errore durante il refresh automatico: {e}")
                self.connect_to_database()
            time.sleep(0.5)

    def handle_manual_input(self):
        while self.running:
            try:
                user_input = input("\nComando (help per la lista comandi): ")
                if user_input.lower() == 'exit':
                    self.running = False
                    break
                elif user_input.lower().startswith('log:'):
                    requested_level = user_input[4:].strip().upper()
                    self.set_log_level(requested_level)
                elif user_input.lower() == 'help':
                    print("\nComandi disponibili:")
                    print("- exit: Chiude il server")
                    print("- log:LEVEL: Cambia il livello di log (DEBUG, INFO, WARNING, ERROR)")
                    print("- help: Mostra questo messaggio di aiuto")
                    print("- stats: Mostra statistiche sulle connessioni")
                    print("- time: Mostra l'ora attuale di Roma")
                    print("- Qualsiasi altro testo: Invia come comando ai client\n")
                elif user_input.lower() == 'stats':
                    print("\nStatistiche del server:")
                    print(f"- Client connessi: {len(self.clients)}")
                    print(f"- Errori registrati: {sum(self.error_counters.values())}")
                    print(f"- Database connesso: {bool(self.db_connection and self.db_connection.is_connected())}")
                    print(f"- Ultimo ID processato: {self.last_processed_id}")
                    print(f"- Refresh abilitato: {self.refresh_enabled}\n")
                elif user_input.lower() == 'time':
                    date_str, time_str = self.get_current_rome_time()
                    print(f"\nOra attuale di Roma: {date_str} {time_str}\n")
                else:
                    self.send_to_client(user_input + '\r\n')
                    self.log("INFO", f"Comando manuale inviato: {user_input}")
            except Exception as e:
                self.log("ERROR", f"Errore nell'invio del comando manuale: {e}")

    def start(self):
        try:
            if not self.connect_to_database():
                self.log("WARNING", "Impossibile connettersi al database. Il server continuerà senza funzionalità database.")
            
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.settimeout(60)
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(5)
            
            date_str, time_str = self.get_current_rome_time()
            self.log("INFO", f"Server TCP avviato su {self.host}:{self.port} - Ora di Roma: {date_str} {time_str}")
            self.log("INFO", "In attesa di connessioni...")
            
            manual_input_thread = threading.Thread(target=self.handle_manual_input, daemon=True)
            manual_input_thread.start()
            
            refresh_thread = threading.Thread(target=self.periodic_refresh, daemon=True)
            refresh_thread.start()
            
            accept_thread = threading.Thread(target=self.accept_connections, daemon=True)
            accept_thread.start()
            
            manual_input_thread.join()
            
        except Exception as e:
            self.log("ERROR", f"Errore nell'avvio del server: {e}")
        finally:
            self.cleanup()

    def accept_connections(self):
        while self.running:
            try:
                client_socket, address = self.server_socket.accept()
                client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                client_socket.settimeout(30)
                
                self.log("INFO", f"Connessione stabilita con race result da {address[0]}:{address[1]}")
                client_thread = threading.Thread(target=self.handle_client, args=(client_socket, address), daemon=True)
                client_thread.start()
            except socket.timeout:
                continue
            except Exception as e:
                if self.running:
                    self.throttled_error_log("accept_conn", f"Errore nell'accettare una connessione: {e}")
                    time.sleep(1)

    def cleanup(self):
        self.running = False
        
        for client in self.clients:
            try:
                client.close()
            except:
                pass
        self.clients.clear()
        
        if self.server_socket:
            self.server_socket.close()
            self.log("INFO", "Socket server chiuso")
        
        self.disconnect_from_database()

if __name__ == '__main__':
    try:
        print("===================================================")
        print("  Server TCP per Race Result - v2.0")
        print("===================================================")
        
        # Richiesta del numero del file con default a 1
        while True:
            try:
                file_number_input = input("Inserisci il numero del file per RaceResult (premere Invio per usare 1): ").strip()
                if not file_number_input:
                    file_number = 1  # Default value
                else:
                    file_number = int(file_number_input)
                break
            except ValueError:
                print("Errore: Inserire un numero valido")
        
        db_config = {
            'host': 'localhost',
            'user': 'root',
            'password': '080502',
            'database': 'rfid'
        }
        
        port = 3601
        if len(sys.argv) > 1:
            try:
                port = int(sys.argv[1])
                print(f"Porta personalizzata specificata: {port}")
            except ValueError:
                print(f"Porta non valida. Utilizzo della porta predefinita: {port}")
        
        print(f"Avvio del server TCP sulla porta {port}...")
        print("Questo server sostituisce Virtual Serial Port Emulator (VSPE)")
        print("Configurare Race Result per connettersi direttamente a questo server TCP")
        print("\nComandi disponibili durante l'esecuzione:")
        print("- log:LEVEL - Cambia il livello di log (DEBUG, INFO, WARNING, ERROR)")
        print("- time - Mostra l'ora attuale di Roma")
        print("- exit - Chiude il server")
        print("- help - Mostra l'elenco dei comandi")
        print("- stats - Mostra statistiche del server")
        
        rome_tz = pytz.timezone('Europe/Rome')
        now = datetime.datetime.now(rome_tz)
        print(f"\nOra attuale di Roma: {now.strftime('%Y-%m-%d %H:%M:%S')}")
        
        server = TCPServer(port=port, db_config=db_config, user_login='D-59665', 
                         keep_alive_seconds=5, error_log_throttle=60)
        server.set_file_number(file_number)
        server.set_log_level("INFO")
        print(f"\nNumero file RaceResult impostato a: {file_number}")
        server.start()
    except KeyboardInterrupt:
        print("\nInterruzione da tastiera. Arresto del server...")
    except Exception as e:
        print(f"Errore durante l'avvio del server: {e}")
        input("Premere Invio per terminare...")
import os
import socket
import threading
import sys
import time
from protocoloTCP import*
from protocoloUDP import*

ip_server = '10.4.4.1' #ip do tracker
porta_server = 9090
porta_client = 12345

n = 10 #número de conexões em fila
chunk_size = 1000 #bytes

file_lock = threading.Lock()
file_dict = {}

lock_TCP = threading.Lock()


###########################################################
#                Tracker Protocol
###########################################################

#Atualiza o dicionário de ficheiros
def atualiza_file_dict(socket_TCP):
    with lock_TCP:
        _,payload = parser_msg(socket_TCP)
    id_ficheiro = int.from_bytes(payload[:4], byteorder="big")
    nome = payload[4:].decode('utf-8')
    with file_lock:
        if id_ficheiro not in file_dict:
            lock = threading.Lock()
            file_dict[id_ficheiro] = (nome,lock)
            #print(file_dict)



#envia informações sobre os ficheiros que tem na pasta
def guarda_inicial(pasta, socket_TCP):
    if not os.path.exists(pasta):
        print(f"'{pasta}' does not exist rip.")
        return
    conteudo = []
    for ficheiro in os.listdir(pasta):
        caminho = os.path.join(pasta, ficheiro)
        tamanho = os.path.getsize(caminho)

        handle_guarda_total(ficheiro, tamanho,socket_TCP)



#Função que informa de ficheiro completo
def handle_guarda_total (nome_ficheiro, tamanho, socket_TCP):
    #enviar informação do ficheiro que tem
    conteudo_bytes = b''
    conteudo_bytes += int(True).to_bytes(1, byteorder="big") #flag indica que todo o ficheiro está a ser enviado
    conteudo_bytes += tamanho.to_bytes(4, byteorder="big")
    conteudo_bytes += nome_ficheiro.encode('utf-8')
    with lock_TCP:
        socket_TCP.send (mensagem(GUARDA, conteudo_bytes))
    atualiza_file_dict(socket_TCP)



#Função que informa de chunk de um ficheiro
def handle_guarda_parcial (nome_ficheiro, chunk, socket_TCP):
    conteudo_bytes = b''
    conteudo_bytes += int(False).to_bytes(1, byteorder="big")
    conteudo_bytes += chunk.to_bytes(4, byteorder="big")
    conteudo_bytes += nome_ficheiro.encode('utf-8')
    with lock_TCP:
        socket_TCP.send (mensagem(GUARDA, conteudo_bytes))
    #atualiza_file_dict(socket_TCP)



#Pede informações ao tracker sobre o ficheiro pretendido
def handle_get(socket_TCP, data_all, pasta):
    with lock_TCP:
        socket_TCP.send(mensagem(GET, data_all[0].encode('utf-8')))
        flag, nodes = parser_msg(socket_TCP)
    
    list_nodes = nodes.split(b'##Separador##')

    if list_nodes == [b'']:
        print(f"Arquivo {data_all[0]} não encontrado")
        return

    threading.Thread(target=pede_ficheiro, args=(list_nodes, data_all[0], socket_TCP, pasta)).start()



# Fecha o node
def handle_quit(socket_TCP, data_all, pasta):
    socket_TCP.close()
    sys.exit(0)



# Distribui o imput do utilizador pelas respetivas funções
def handle_input(socket_TCP, input, pasta):
    inputs = {
        'get' : handle_get,
        'quit' : handle_quit,
        'q' : handle_quit
    }
    command = input.split(' ')
    if (command[0] not in inputs):
        print("Comando inválido")
        return
    inputs[command[0]](socket_TCP,command[1:], pasta)



###########################################################
#                Transfer Protocol
###########################################################

#Pede chunks aos outros nodos
def pede_ficheiro (list_nodes, nome_ficheiro, socket_TCP, pasta):

    # Cria um socket UDP
    socket_pedidos_UDP = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    #Separa a informação recebida do tracker
    reconstructed_dict = {}
    id = int.from_bytes(list_nodes[0], 'big')
    tamanho = int.from_bytes(list_nodes[1], 'big')
    for i in range(2, len(list_nodes), 2):
        nome = list_nodes[i].decode('utf-8') + ".cc"
        
        #chunks = list(map(int.from_bytes, list_nodes[i + 3].split(b' ')))
        chunks = [int.from_bytes(chunk, byteorder='big') for chunk in list_nodes[i + 1].split(b'#s#')]
        
        ip = socket.gethostbyname(nome)
        reconstructed_dict[ip] = chunks
        #print(f"Node ({nome}) tem o ID {id}, tamanho {tamanho} do arquivo {nome_ficheiro} e possui os chunks: {chunks}")
  
    #Soloca o novo ficheiro no file_dict
    with file_lock:
        lock = threading.Lock()
        file_dict[id] = (nome_ficheiro,lock)

    #Abre o ficheiro com o tamanho correto
    caminho = os.path.join(pasta, nome_ficheiro)
    with open(caminho, 'wb') as novo_arquivo:
        novo_arquivo.seek(tamanho - 1)
        novo_arquivo.write(b'\0')

    #print (reconstructed_dict)

    n_chunks = (tamanho + chunk_size - 1) // chunk_size
    
    thread1 = threading.Thread(target=get_chunks, args=(socket_pedidos_UDP, reconstructed_dict, id, n_chunks))
    thread1.start()

    #receber sends através de threads
    threads = []
    chunks_recebidos = []

    #thread que lida com as perdas
    thread2 = threading.Thread(target=get_missing_chunks, args=(socket_pedidos_UDP, chunks_recebidos, reconstructed_dict, id, n_chunks))        
    thread2.start()

    while len(chunks_recebidos) < n_chunks:
        id_ficheiro, chunk, payload, _ = parser_dtg (socket_pedidos_UDP)
        if id_ficheiro != None:
            if chunk not in chunks_recebidos: # lida com duplicações
                thread = threading.Thread(target=funcao_received_send, args=(socket_TCP, id_ficheiro, chunk, payload, pasta))
                thread.start()
                threads.append(thread)
                chunks_recebidos.append(chunk)

    print("Ficheiro completo")
    socket_pedidos_UDP.close()



#Função que pede uma vez cada chunk a algum node
def get_chunks (socket_pedidos_UDP, reconstructed_dict, id, n_chunks):
    chunk_atual = 1

    while (chunk_atual <= n_chunks):

        for key in reconstructed_dict:

            if chunk_atual in reconstructed_dict[key]:
                print(f"vou pedir o chunk {chunk_atual} ao Node {key}")
                time.sleep(0.05)
                socket_pedidos_UDP.sendto(datagrama(id, chunk_atual, "".encode("utf-8")), (key,12345))
                chunk_atual += 1
            
            if (chunk_atual > n_chunks):
                break



#Função que passado algum tempo volta a fazer pedidos do chunks em falta (lida com perdas)
def get_missing_chunks (socket_pedidos_UDP, chunks_recebidos, reconstructed_dict, id, n_chunks):
    timeout = 5 + n_chunks/20
    time.sleep(timeout)
    
    while len(chunks_recebidos) < n_chunks:
        print("A pedir chunks em falta")

        chunk_atual = 1
        while (chunk_atual <= n_chunks):

            for key in reconstructed_dict:
                
                if chunk_atual not in chunks_recebidos:
                
                    if chunk_atual in reconstructed_dict[key]:
                        print(f"vou pedir o chunk {chunk_atual} ao Node {key}")
                        socket_pedidos_UDP.sendto(datagrama(id, chunk_atual, "".encode("utf-8")), (key,12345))
                        chunk_atual += 1
            
                else:
                    chunk_atual += 1

                if (chunk_atual > n_chunks):
                    break
        
        time.sleep(timeout)



# Função que recebe as informações de um get e envia a parte correspondente desse ficheiro
def funcao_received_get (socket_UDP, client_socket, id_ficheiro, chunk, pasta):
    if not os.path.exists(pasta):
        print(f"'{pasta}' does not exist")
    else:   
        offset = calcular_offset(chunk, chunk_size)
        with file_lock:
            nome_ficheiro, lock_ficheiro = file_dict[id_ficheiro]
        if nome_ficheiro in os.listdir(pasta):
            caminho = os.path.join(pasta, nome_ficheiro) 
            tamanho_ficheiro = os.path.getsize(caminho)

            if offset < tamanho_ficheiro:
                with open(caminho, 'rb') as ficheiro:
                    with lock_ficheiro:
                        ficheiro.seek(offset)
                        chunk_data = ficheiro.read(chunk_size)
                
                # Envia um send para o outro cliente
                print (f"A enviar{chunk}")
                socket_UDP.sendto(datagrama(id_ficheiro, chunk, chunk_data), client_socket)

            else:
                print(f"Offset inválido")
        else: 
            print(f"Ficheiro não encontrado")



# Reconstroi parte do ficheiro
def funcao_received_send (socket_TCP, id_ficheiro, chunk, payload, pasta):
    with file_lock:
        nome_ficheiro, lock_ficheiro = file_dict[id_ficheiro]
    caminho = os.path.join(pasta, nome_ficheiro)

    if not os.path.exists(pasta):
        print(f"'{pasta}' does not exist")

    else:
        if nome_ficheiro:
            offset = calcular_offset(chunk, chunk_size)
            with open(caminho,'r+b') as ficheiro:
                with lock_ficheiro:
                    ficheiro.seek(offset,0)
                    ficheiro.write(payload)

            #Informa a chegada do chunk
            print(f"Chunk {chunk} do arquivo {nome_ficheiro} recebido com sucesso.")

            #Envia um guarda_parcial para o tracker
            threading.Thread(target=handle_guarda_parcial, args=(nome_ficheiro,chunk,socket_TCP)).start()

        else:
            print(f"Ficheiro não encontrado")



# Função que cria um socket que recebe pedidos de chunks de outros nodes
def comunicacao_entre_nodos(endereco, pasta): 
    socket_UDP = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    global porta_client
    socket_UDP.bind((endereco,porta_client))
    print(f"Aguardando mensagens em {endereco}")

    while True:
        id_ficheiro, offset, data_all, client_socket = parser_dtg(socket_UDP)
        if not data_all:
            time.sleep(0.1)
            
        thread_worker = threading.Thread(target=funcao_received_get, args=(socket_UDP, client_socket, id_ficheiro, offset, pasta))
        thread_worker.start()



###########################################################
#                       MAIN
###########################################################

def main(): #Argumento 1: Pasta dos Ficheiros; Argumento 2: o nome do cliente que executa
    pasta = sys.argv[1]
    nome = sys.argv[2]

    #Resolve o nome fornecido para o seu endereço ip
    try:
        endereco = socket.gethostbyname(nome)
    except socket.gaierror as e:
        print(f"Erro ao obter o endereço IP para {nome}: {e}")
        sys.exit(1)

    threading.Thread(target=comunicacao_entre_nodos, args=(endereco, pasta,), daemon=True).start()
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #Estabelece conexao com o tracker
    s.connect((ip_server, porta_server))
    guarda_inicial(pasta,s)
    
    while True:
        user_input = input("")
        if user_input:
            handle_input(s, user_input, pasta)


if __name__ == "__main__":
    main()
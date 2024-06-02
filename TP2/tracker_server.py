import os
import socket
import threading
import sys
from protocoloTCP import*

ip_server = '10.4.4.1' #ip do tracker
porta_server = 9090

n = 10 #número de conexões em fila
chunk_size = 1000 #bytes

node_super_dick = {}
lock_node_super_dick = threading.Lock()

file_lock = threading.Lock()
file_dict = {}
file_id = 1

#Chama a função correspondente à flag recebida
def emparelha(node_socket,nome_host): #Distribui o socket para a flag correta
    flags_hand = { #Dispatchure leva te da flag ate a função
        GUARDA: guarda_hand,
        GET: get_hand,
    }

    while True:
        flag, data_all = parser_msg(node_socket)
        if not data_all:
            break
        flags_hand[flag](node_socket, nome_host, data_all) 

    #Retira do dicionário do tracker as informações do nodo para quando acabar conexão
    global node_super_dick
    with lock_node_super_dick:
        for ficheiro in node_super_dick:
            if nome_host in node_super_dick[ficheiro]:
                del node_super_dick[ficheiro][nome_host]

    node_socket.close() #Fecha a conexão



#Função que atualiza o node_super_dick
def guarda_hand(node_socket,nome_host,data_all):
    global node_super_dick
    
    total = bool(int.from_bytes(data_all[:1], byteorder="big"))
    
    if total:
        #Tamanho e chunks
        tamanho = int.from_bytes(data_all[1:5], byteorder="big")
        chunknumb = (tamanho + chunk_size - 1) // chunk_size

        nome_ficheiro = data_all[5:].decode('utf-8')

        #Atualiza file_dict
        global file_id
        with file_lock:
            if nome_ficheiro not in file_dict:
                file_dict[nome_ficheiro] = (file_id,tamanho)
                file_id += 1
                #print(file_dict)

        with lock_node_super_dick:
            if nome_ficheiro in node_super_dick:
                # Se o arquivo já existe, adiciona o node_socket ao dicionário interno
                node_super_dick[nome_ficheiro][nome_host] = list(range(1, chunknumb + 1))
            else:
                # Se o arquivo não existe, cria um novo dicionário interno
                node_super_dick[nome_ficheiro] = {nome_host: list(range(1, chunknumb + 1))}

        #Responder ao pedido com o id do ficheiro
        with file_lock:
            n = file_dict.get(nome_ficheiro)[0]
        conteudo_bytes = b''
        conteudo_bytes += n.to_bytes(4, byteorder="big")
        conteudo_bytes += nome_ficheiro.encode('utf-8')
        node_socket.send (mensagem(GUARDA, conteudo_bytes))

    else:
        chunk = int.from_bytes(data_all[1:5], byteorder="big")
        nome_ficheiro = data_all[5:].decode('utf-8')

        with lock_node_super_dick:
            if nome_ficheiro in node_super_dick:
                # Se o arquivo já existe, adiciona o chunk à lista existente
                if nome_host in node_super_dick[nome_ficheiro]:
                    node_super_dick[nome_ficheiro][nome_host].append(chunk)
                else:
                    node_super_dick[nome_ficheiro][nome_host] = [chunk]
            
            #print(node_super_dick)



# Função que envia as informações sobre um determinado ficheiro para o node que a pediu
def get_hand(node_socket,nome_host,data_all):
    file = data_all.decode('utf-8')
    
    with file_lock:    
        if file in file_dict:
            id = file_dict.get(file)[0]
            tamanho = file_dict.get(file)[1]
        else:
            id = -1

    with lock_node_super_dick:
        if file in node_super_dick:
            ip = node_super_dick.get(file)
        else:
            id = -1

        #print(node_super_dick)
    nodes = []
    
    added = 0
    if id != -1:
        nodes.append(id.to_bytes(4, byteorder='big'))
        nodes.append(tamanho.to_bytes(4, byteorder='big'))

        for peer, chunks in ip.items():
            if peer != nome_host:
                added += 1
                nodes.append(peer.encode('utf-8'))
                nodes.append(b'#s#'.join(map(lambda x: x.to_bytes(4, byteorder='big'), chunks)))
    
    if added == 0:
        nodes = []
    #Envia mensagem tcp para o cliente
    node_socket.send(mensagem(SEND, b'##Separador##'.join(nodes)))



###########################################################
#                       MAIN
###########################################################
    
def main ():
    s_server_connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s_server_connection.bind((ip_server, porta_server))
    s_server_connection.listen(n)
    print ("Servidor")

    while True:
        #Aceita a conexao e manda para outro socket dedicado ao novo nodo e associado a uma thread
        node_socket,node_ip = s_server_connection.accept()

        #Resolve o endereço ip obtido no accept() para o nome do host que passará a armazenar
        #Feito aqui para evitar estar sempre a chamar a mesma função
        try:
            endereco = node_socket.getpeername()
            nome_host = socket.gethostbyaddr(endereco[0])[0].split(".")[0]
        except socket.herror as e:
            print(f"Erro de resolução de endereço {endereco}: {e}")
            return None
        
        print("Conexão estabelecida com o Nodo ", nome_host," no endereço ", node_ip)

        #Atribui uma função a thread(emparelha) e os respetivos argumentos(node_socket e nome_host) 
        thread_node = threading.Thread(target=emparelha, args=(node_socket,nome_host)) 
        thread_node.start()

#Chama a main de início
if __name__ == "__main__":
    main()
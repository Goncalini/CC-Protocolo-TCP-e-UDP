# Define message flags
GET = 0x01 #flags para o protocolo saber o serviço
SEND  = 0x02
GUARDA = 0x03


#Função gera mensagem tcp
#flag - 1 byte, tamanho payload - 4 bytes, data_all (payload)
def mensagem(flag,data_all):
    return bytearray([flag]) + len(data_all).to_bytes(4, byteorder='big') + data_all
  


#Separa mensagem tcp
def parser_msg(socket):
    data = socket.recv(1024)
    if not data:
        return None, None
    
    flag, data_length, data_all = data[0], data[1:5], data[5:]
    data_lengt_int = int.from_bytes(data_length, "big")

    if data_lengt_int > 1024 - 5:
        while True:
            data = socket.recv(1024)
            if not data:
                break
            print ("\n")
            data_all += data
            if len(data_all) == data_lengt_int:
                break
    return flag, data_all #data all é o payload
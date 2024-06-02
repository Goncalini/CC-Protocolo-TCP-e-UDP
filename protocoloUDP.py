import zlib

chunk_size = 1000 #bytes



#FUNÇOES AUXILIARES
#para um certo chunk calcula o seu offset
def calcular_offset(n_chunk, chunk_size):
    return (n_chunk-1) * chunk_size


#calcula o numero do chunk apartir do offset
def calcular_n_chunk(offset,chunk_size):
    return (offset // chunk_size) + 1



#Calcula o valor do checksum, com todos os elementos do datagrama exceto ele próprio
def calcular_checksum(*args):
    data = b''.join(args)
    checksum = zlib.crc32(data) & 0xFFFFFFFF
    return checksum.to_bytes(4, byteorder='big')



#Função gera datagrama udp e dá formato ao seu cabeçalho
#checksum - 4 bytes, id ficheiro - 4 bytes, offset - 4 bytes, tamanho payload - 4 bytes, data all (payload)
def datagrama(id_file,offset,data_all):
    id_ficheiro = id_file.to_bytes(4, byteorder='big')
    offset_ficheiro = offset.to_bytes(4,byteorder='big')
    tamanho_payload = len(data_all).to_bytes(4, byteorder='big')
    
    checksum = calcular_checksum(id_ficheiro,offset_ficheiro,tamanho_payload,data_all)

    return bytearray(checksum) + id_ficheiro + offset_ficheiro + tamanho_payload + data_all



#Separa um datagrama udp nos seus campos. Faz a deteção de erros(checksum)
def parser_dtg(socket):
    data, client_socket = socket.recvfrom(1024)
    if not data:
        return None, None, None, None

    checksum,id_ficheiro, offset, data_length, data_all = data[0:4], data[4:8], data[8:12], data[12:16], data[16:]
    #Passa elementos necessários para de bytes para int para serem devolvidos no final
    data_length_int = int.from_bytes(data_length, "big")
    id_ficheiro_int = int.from_bytes(id_ficheiro, "big")
    offset_int = int.from_bytes(offset, "big")

    if data_length_int > 1024 - 16:
        while True:
            data = socket.recv(1024)
            if not data:
                break
            data_all += data
            if len(data_all) == data_length_int:
                break
    
    #Calculo do Checksum -> se o calculado for diferente do que tem, dá erro!
    expected_checksum = calcular_checksum(id_ficheiro, offset, data_length, data_all)

    if checksum != expected_checksum:
        print("Erro de checksum. Os dados podem estar corrompidos.")
        return None, None, None, None

    return id_ficheiro_int, offset_int, data_all, client_socket
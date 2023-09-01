#imports
import mysql.connector
from alive_progress import alive_bar
import os
import subprocess
import time


#variaveis globais
global tempo_total

#funcao que recebe a string e printa ela em vermleho
def printError(string):
    print("\033[1;31m" + string + "\033[0;0m")

#funcao que recebe a string e printa ela em verde
def printSuccess(string):
    print("\033[1;32m" + string + "\033[0;0m")

#----------------------Funções referentes aos dados de conexão----------------------#

def pegaDadosDb():
    print("Digite os dados referentes ao banco de dados que você deseja copiar:")
    host_db = input("Host: ")
    port_db = input("Porta: ")
    user_db = input("Usuário: ")
    passwd_db = input("Senha: ")
    return host_db, port_db, user_db, passwd_db, 

def pegaDadosLocalhost():
    print("Digite os dados referentes ao localhost:")
    host_local = input("Host: ")
    port_local = input("Porta: ")
    user_local = input("Usuário: ")
    passwd_local = input("Senha: ")
    return host_local, port_local, user_local, passwd_local

#--------------------------------Funções principais--------------------------------#
def makeDump(host_db, port_db, user_db, passwd_db, database_copiada):
    global tempo_total
    tabela_ignorada = ""
    force = ""

    # ============= faz verificações sobre o que o usuário deseja ignorar / incluir no dump =============
    
    if int(input("Deseja ignorar alguma tabela? \n| (1) -> Sim |   | (0) -> Não | -> ")) == 1:
        while True:
            tabela_ignorada += " --ignore-table=" + database_copiada + "." + input("Qual tabela você deseja ignorar? ")
            escolha = int(input("Deseja ignorar mais alguma tabela? \n| (1) -> Sim |   | (0) -> Não | -> "))
            if escolha == 0:
                break
   
    #verifica se deseja executar o dump no modo force (ignora possiveis erros)
    if (int(input("Deseja executar no modo Force? (ATENÇÃO: o modo force, apesar de o mostrar ao usuário, ignora possiveis erros ocorridos ao gerar o dump. Pode ser util caso esteja enfrentando erros em apenas uma tabela especifica que não seja de tamanha importancia) \n| (1) -> Sim |   | (0) -> Não | -> ")) == 1):
         force = "--force"
    
    dump = (f"mysqldump -h{host_db} -P{port_db} -u{user_db} -p{passwd_db} --extended-insert --single-transaction {force} {database_copiada}{tabela_ignorada}")

    # ==================== faz o dump do database ====================

    try:
        aux_time = time.time()
        subprocess.run(dump, stdout=open(f"{database_copiada}.txt", 'w'))
        tempo_total = (time.time() - aux_time)

    except subprocess.CalledProcessError as e:
        printError("\n!-!-!-!-!-!-!-Erro ao realizar o dump-!-!-!-!-!-!-\n")
        print(e.output)
        printError("-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-")
        return False
    
    else:
        #Verifica se na penultima linha do arquivo esta escrito "-- Dump completed on" para retornar verdadeiro ou falso
        with open(f"{database_copiada}.txt", "r", encoding='ISO-8859-1') as file:
            lines = file.readlines()
            if "-- Dump completed on" in lines[-1]:
                printSuccess("-=-=-=-=-=-=-Arquivo dump gerado com sucesso-=-=-=-=-=-=-")
                return True
            else:
                printError("\n!-!-!-!-!-!-!-Ocorreu um erro ao gerar o arquivo dump-!-!-!-!-!-!-\n")
                return False



def importDump(database_copiada, dbLocalhost):
    cursor = dbLocalhost.cursor()
    cursor.execute(f"DROP DATABASE IF EXISTS {database_copiada}")
    cursor.execute(f"CREATE DATABASE {database_copiada}")
    cursor.execute(f"USE {database_copiada}")
    cursor.close()

    #caso o tamanho do arquivo seja maior que 50mb, vai dividindo por linhas
    if os.path.getsize(f"{database_copiada}.txt") > 50000000:
        print("Arquivo muito grande, dividindo por linhas...")

        arquivo = open(f"{database_copiada}.txt", "r", encoding='ISO-8859-1')
        conteudo = arquivo.read()
        arquivo.close()

        lista_comandos = []
        lista_comandos = conteudo.split(";\n")

        print("lista de comandos criada!\nIniciando inserções no banco de dados...\nAguarde...")

        cursor = dbLocalhost.cursor()
        
        with alive_bar (len(lista_comandos)) as bar:
            for c in lista_comandos:
                bar()
                try:
                    cursor.execute(c)
                except Exception as e:
                    printError(f"Erro ao executar o comando: {c[:50]}...")
                    print(e)

        print("-=-=-=-=-=-=-Importação finalizada-=-=-=-=-=-=-")
        cursor.close()
        dbLocalhost.commit()

    else:
        cursor = dbLocalhost.cursor()
        try:
            with open(f"{database_copiada}.txt", "r", encoding='ISO-8859-1') as file:
                cursor.execute(file.read(), multi=True)
        except Exception as e:
            cursor.execute(f"DROP DATABASE IF EXISTS {database_copiada}")
            printError("-!-!-!-!-!-!-Erro ao importar o dump-!-!-!-!-!-!-")
            print(e)
            printError("-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-")
            cursor.close()
        else:
            printSuccess("-=-=-=-=-=-=-Importação realizada com sucesso-=-=-=-=-=-=-")
            cursor.close()




#--------------------------------Função principal--------------------------------#
def main():
    #variaveis de conexão com o banco de dados
    host_db     = "000.00.0.000"
    port_db     = "3306"
    user_db     = "root"
    passwd_db   = "root"

    host_local  = "localhost"
    port_local  = "3306"
    user_local  = "root"
    passwd_local= "root"


    print("Deseja inserir os dados da conexão manualmente ou utilizar a opção default?")
    print(f"Opção default: | Host: {host_db} | Porta: {port_db} | Usuário: {user_db} | ") 
    opcao = int(input("| (1) -> inserir |   | (0) -> Default | -> "))
    if opcao == 1:
        host_db, port_db, user_db, passwd_db = pegaDadosDb()

    print("Deseja inserir as informações do localhost manualmente ou utilizar a opção default?")
    print(f"Opção default: | Host: {host_local} | Porta: {port_local} | Usuário: {user_local} | Senha: {passwd_local} |")
    opcao = int(input("| (1) -> inserir |   | (0) -> Default | -> "))
    if opcao == 1:
        host_local, port_local, user_local, passwd_local = pegaDadosLocalhost()



    while True:
        global tempo_total
        tempo_total = 0

        database_copiada = input("Qual database você deseja copiar?\n")

        #faz a conexão com o localhost
        dbLocalhost = mysql.connector.connect(
            host    = host_local,
            user    = user_local,
            passwd  = passwd_local,
            port    = port_local
        )
        
        if(makeDump(host_db, port_db, user_db, passwd_db, database_copiada)):
            aux_time = time.time()

            importDump(database_copiada, dbLocalhost)

            tempo_total += (time.time() - aux_time)
            print(f"Tempo total: {tempo_total} segundos")

            os.remove(f"{database_copiada}.txt")
        else:
            os.remove(f"{database_copiada}.txt")

        escolha = int(input("Deseja copiar mais algum database? \n| (1) -> Sim |   | (0) -> Não | -> "))
        if escolha == 0:
            break

    dbLocalhost.close()

try:
    main()
except Exception as e:
    printError("Ocorreu um erro inesperado")
    print(e)
finally:
    input("Pressione enter para sair...")
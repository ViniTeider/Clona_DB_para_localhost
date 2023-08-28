import mysql.connector
import os
import subprocess

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
    escolha = int(input("Deseja ignorar alguma tabela? \n| (1) -> Sim |   | (0) -> Não |\n-> "))
    tabela_ignorada = ""
    
    if escolha == 1:
        while True:
            tabela_ignorada += " --ignore-table=" + database_copiada + "." + input("Qual tabela você deseja ignorar? ")
            escolha = int(input("Deseja ignorar mais alguma tabela? \n| (1) -> Sim |   | (0) -> Não |\n-> "))
            if escolha == 0:
                break
    
    dump = (f"mysqldump -h{host_db} -P{port_db} -u{user_db} -p{passwd_db} --add-drop-database --add-drop-table --set-charset  {database_copiada}{tabela_ignorada}")

    try:
        subprocess.run(dump, stdout=open(f"{database_copiada}.sql", 'w'))
    
    except subprocess.CalledProcessError as e:
        os.remove(f"{database_copiada}.sql")
        print("\n!-!-!-!-!-!-!-Erro ao realizar o dump-!-!-!-!-!-!-\n")
        print(e.output)
        return False
    
    else:
        print("-=-=-=-=-=-=-Dump realizado com sucesso-=-=-=-=-=-=-")
        return True 

def importDump(database_copiada, dbLocalhost):
    #cria o database_copiada no localhost
    cursor = dbLocalhost.cursor()

    cursor.execute(f"DROP DATABASE IF EXISTS {database_copiada}")
    cursor.execute(f"CREATE DATABASE {database_copiada}")
    cursor.execute(f"USE {database_copiada}")

    try:
        with open(f"{database_copiada}.sql", "r") as file:
            cursor.execute(file.read())
    except:
        print("-!-!-!-!-!-!-Erro ao importar o dump-!-!-!-!-!-!-")
        cursor.execute(f"DROP DATABASE IF EXISTS {database_copiada}")
    else:
        print("-=-=-=-=-=-=-Importação realizada com sucesso-=-=-=-=-=-=-")

    cursor.close()
    os.remove(f"{database_copiada}.sql")


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
    print(f"Opção default: | Host: {host_db} | Porta: {port_db} | Usuário: {user_db} | Senha: {passwd_db} |") 
    opcao = int(input("| (1) -> inserir |   | (0) -> Default |\n-> "))
    if opcao == 1:
        host_db, port_db, user_db, passwd_db = pegaDadosDb()

    print("Deseja inserir as informações do localhost manualmente ou utilizar a opção default?")
    print(f"Opção default: | Host: {host_local} | Porta: {port_local} | Usuário: {user_local} | Senha: {passwd_local} |")
    opcao = int(input("| (1) -> inserir |   | (0) -> Default |\n-> "))
    if opcao == 1:
        host_local, port_local, user_local, passwd_local = pegaDadosLocalhost()


    #faz a conexão com o localhost
    dbLocalhost = mysql.connector.connect(
        host    = host_local,
        user    = user_local,
        passwd  = passwd_local,
        port    = port_local
    )

    #pede ao usuario qual db ele quer
    database_copiada = input("Qual database você deseja copiar?\n")

    if(makeDump(host_db, port_db, user_db, passwd_db, database_copiada)):
        importDump(database_copiada, dbLocalhost)

    dbLocalhost.close()
    # dbLocalhost.commit()

main()
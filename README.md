Em andamento: irei alimentar esse repositório constantemente ao passar do tempo.

InsertSQL Class
Descrição
A classe InsertSQL facilita a execução de consultas e inserções em um banco de dados MySQL a partir de um script Python. Ela permite estabelecer uma conexão com o banco de dados utilizando informações armazenadas em um arquivo JSON e executar consultas SQL diretamente.

Funcionalidades
Leitura de Configuração: Lê as informações de conexão do banco de dados a partir de um arquivo JSON.
Conexão com o Banco de Dados: Estabelece uma conexão segura com o banco de dados MySQL utilizando os dados lidos do arquivo de configuração.
Execução de Consultas: Permite a execução de consultas SQL (inserção, atualização, etc.) e consultas que retornam dados do banco.
Estrutura do Projeto
plaintext
Copiar código
project_root/
│
├── src/
│   ├── mysql/
│   │   └── sql_insertion.py   # Script principal contendo a classe InsertSQL
│
├── V:/00_CONF_ROBOS/MYSQL/Conection/
│   └── mysql_config.json      # Arquivo JSON com as informações de conexão
│
└── README.md                  # Arquivo de documentação
Como Utilizar
Instalação
Certifique-se de que o pacote mysql-connector-python esteja instalado. Caso não esteja, você pode instalá-lo usando pip:

sh
Copiar código
pip install mysql-connector-python
Exemplo de Uso
python
Copiar código
from src.mysql.sql_insertion import InsertSQL

# Defina sua query SQL
query = "INSERT INTO my_table (column1, column2) VALUES ('value1', 'value2')"

# Instancie a classe com a query
inserter = InsertSQL(query)

# Executar uma inserção no banco de dados
inserter.mysql_insert(host='host', port='port', user='user', password='password', database='database')

# Executar uma consulta no banco de dados
resultados = inserter.mysql_query(host='host', port='port', user='user', password='password', database='database')
print(resultados)
Configuração JSON
O arquivo JSON deve conter as seguintes chaves com as respectivas informações de conexão:

json
Copiar código
{
    "host": "seu_host",
    "port": "sua_porta",
    "user": "seu_usuario",
    "password": "sua_senha",
    "database": "seu_banco_de_dados"
}
Métodos Disponíveis
read_connection_info(filename): Lê e retorna as informações de conexão de um arquivo JSON.
connection(host, port, user, password, database): Estabelece uma conexão com o banco de dados MySQL.
mysql_insert(host, port, user, password, database): Executa uma consulta SQL que altera dados no banco (INSERT, UPDATE, DELETE).
mysql_query(host, port, user, password, database): Executa uma consulta SQL que retorna dados do banco.
Requisitos
Python 3.x
Módulos: mysql-connector-python, json, sys, os

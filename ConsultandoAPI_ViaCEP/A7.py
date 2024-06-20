# %%
#Espaco para imports
import requests #Pip install requests ou no codigo > !Pip install requests
import pandas as pd
import db_Connection as dbcon
import json

# %%
#Lista de ceps a serem procurados
lista_ceps = ['01153000', '20050000', '70714020']

# %%
#lista onde armazenaremos os enderecos
lista_enderecos = []

# %%
#Faz requisição na API pegando os dados de endero de cada CEP

for cep in lista_ceps:
    
    url: str = "https://viacep.com.br/ws/{}/json/".format(cep)
    
    try:
        req = requests.get(url, timeout=5)

        if(req.status_code == 200):
            endereco = req.json()            

            #Mona
            lista_enderecos.append(
                {
                    "cep": endereco['cep'],
                    "logradouro": endereco['logradouro'],
                    "complemento" : endereco['complemento'],
                    "bairro" : endereco['bairro'],
                    "cep" :endereco['cep'],
                    "localidade" : endereco['localidade'],
                    "uf" :endereco['uf']
                }
            )
        else:
            erro = req.raise_for_status()
            print(f"Erro ao buscar o Cep{cep}: {erro}")
    except:
        erro = req.raise_for_status()
        print(f"Erro ao buscar o Cep{cep}: {erro}")

# %%
for item in lista_enderecos:
    print(item)

# %%
#Salvando Json em arquivo .json

try:
    with open('enderecos.json', 'w', encoding='utf-8') as json_file:
        json.dump(lista_enderecos, json_file, ensure_ascii=False, indent=4)
except Exception as e:
    print(f"Erro ao gravar arquivos json {e}")


# %%
#Insere Registros no BAnco de dados criando uma nova tabela caso não existe 

df_enderecos = pd.DataFrame(lista_enderecos)

df_enderecos.to_sql("TB_ENDERECO", dbcon.engine, if_exists='replace', index=False)




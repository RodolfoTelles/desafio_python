import requests
import zipfile
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import re

# URL base do diretório
base_url = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/"

# PATH onde os arquivos serão salvos e extraídos
destino = "./data_files/"

# Faz o request da página dados abertos CNPJ
response_dt = requests.get(base_url)
soup_dt = BeautifulSoup(response_dt.text, 'html.parser')

# Extrai todas as datas
links = soup_dt.find_all('a', href=re.compile(r'\d{4}-\d{2}/'))
# Data mais recente
v_dir = links[len(links)-1].text
print(f"Diretório mais recente: {v_dir}")

# Faz o request no diretório com data mais recente
response = requests.get(base_url + v_dir)
soup = BeautifulSoup(response.text, 'html.parser')

# Extrai todos os links de arquivos EmpresasX.zip e seleciona o ultimo
empresas = soup.find_all('a', href=re.compile(r'Empresas.\.zip'))
# ultimo arquivo (menor)
# v_empresa = empresas[len(empresas)-1].text
# primeiro arquivo (maior)
v_empresa = empresas[2].text
print(f"Baixando: {v_empresa}")

# Faz o download
res = requests.get(base_url + v_dir + v_empresa, stream=True)
with open(destino + v_empresa, 'wb') as f:
    for chunk in res.iter_content(chunk_size=8192):
        f.write(chunk)

# Extrai CSV do ZIP 
with zipfile.ZipFile(destino + v_empresa, 'r') as zip_ref:
    zip_ref.extractall(destino)

print(f"Download {v_empresa} concluído e CSV extraído.")



# Extrai todos os links de arquivos SociosX.zip e seleciona o ultimo
socios = soup.find_all('a', href=re.compile(r'Socios.\.zip'))
# ultimo arquivo (menor)
# v_socio = socios[len(socios)-1].text
# primeiro arquivo (maior)
v_socio = socios[2].text
print(f"Baixando: {v_socio}")

# Faz o download
res = requests.get(base_url + v_dir + v_socio, stream=True)
with open(destino + v_socio, 'wb') as f:
    for chunk in res.iter_content(chunk_size=8192):
        f.write(chunk)

# Extrai CSV do ZIP 
with zipfile.ZipFile(destino + v_socio, 'r') as zip_ref:
    zip_ref.extractall(destino)

print(f"Download {v_socio} concluído e CSV extraído.")
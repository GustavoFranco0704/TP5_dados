import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3

response = requests.get('https://www.sympla.com.br/')
content = response.content.decode('utf-8')

site = BeautifulSoup(content, 'html.parser')

metadados = site.findAll(
    'a', attrs={'class': 'EventCardstyle__CardLink-sc-1rkzctc-3 eDXoFM sympla-card'})

palavras_ao_ar_livre = ['parque', 'praça', 'viaduto',
                        'autódromo', 'campo', 'vila', 'concha', 'araujo']
palavras_fechado = ['teatro', 'cinema', 'cine', 'salles', 'auditório',
                    'convention', 'armazem', 'casa', 'shopping', 'salão']

palavras_teatro = ['turandot', 'teatro', 'drama', 'peça', 'cena',
                   'entrevista', 'porco', 'sinpro', 'concertos', 'se minha vida']
palavras_show = ['show', 'música', 'samuel rosa',
                 'jean tassy', 'mc cabelinho', 'wagner', 'hosana']
palavras_festival = ['festival', 'kawasaki', 'encontro', 'vinhos']


def identificar_tipo_evento(nome):
    nome_lower = nome.lower() if nome else ""
    if any(palavra in nome_lower for palavra in palavras_teatro):
        return "Teatro"
    elif any(palavra in nome_lower for palavra in palavras_show):
        return "Show"
    elif any(palavra in nome_lower for palavra in palavras_festival):
        return "Festival"
    return "Indefinido"


def identificar_ambiente_evento(local):
    local_lower = local.lower() if local else ""

    if any(palavra in local_lower for palavra in palavras_ao_ar_livre):
        return "Ao ar livre"
    elif any(palavra in local_lower for palavra in palavras_fechado):
        return "Fechado"
    return "Indefinido"


dados_dos_eventos = []

for metadado in metadados:

    link = metadado.get('href')
    print(f'Link Evento: {link}')

    eventos = metadado.findAll(
        'div', attrs={'class': 'EventCardstyle__EventInfo-sc-1rkzctc-5 hRaRCu'})

    for evento in eventos:
        nome_evento = evento.find('h3', attrs={
            'class': 'EventCardstyle__EventTitle-sc-1rkzctc-7 hwgihT animated fadeIn'})
        nome_evento_texto = nome_evento.text if nome_evento else "Nome não disponível"
        print(f'Nome evento: {nome_evento_texto}')

        tipo_evento = identificar_tipo_evento(nome_evento_texto)
        print(f'Tipo do evento: {tipo_evento}')

        data_evento = evento.find(
            'div', attrs={'class': 'sc-1sp59be-1 fZlvlB'})
        data_evento_texto = data_evento.text if data_evento else 'Data não disponível'
        print(f'Data evento: {data_evento.texto}')

        local_evento = metadado.find('div', attrs={
            'class': 'EventCardstyle__EventLocation-sc-1rkzctc-8 heVhPT animated fadeIn'})
        local_evento_texto = local_evento.text if local_evento else "Local não disponível"
        print(f'Local evento: {local_evento_texto}')

        ambiente = identificar_ambiente_evento(local_evento_texto)
        print(f'Ambiente do evento: {ambiente}\n')

        dados_dos_eventos.append({
            "link": link,
            "nome_evento": nome_evento_texto,
            "tipo_evento": tipo_evento,
            "data_evento": data_evento_texto,
            "local_evento": local_evento_texto,
            "ambiente": ambiente
        })


arquivo = pd.DataFrame(dados_dos_eventos, columns=[
                       'link', 'nome_evento', 'tipo_evento', 'data_evento', 'local_evento', 'ambiente'])

arquivo_csv = arquivo.to_csv('dados_eventos.csv', index=False)

ler_arquivo_csv = pd.read_csv('dados_eventos.csv', encoding='utf-8')

# criação banco de dados.

conn = sqlite3.connect("meu_banco.db")
cursor = conn.cursor()


cursor.execute('''
CREATE TABLE IF NOT EXISTS eventos (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nome TEXT NOT NULL,
    tipo TEXT NOT NULL
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS dados_eventos (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    id_evento INTEGER,
    data TEXT,
    localizacao TEXT NOT NULL,
    FOREIGN KEY (id_evento) REFERENCES eventos(id)
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS outros_dados_eventos (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    id_evento INTEGER,
    metadado TEXT,
    FOREIGN KEY (id_evento) REFERENCES eventos(id)
)
''')

cursor.execute('DELETE FROM eventos')
cursor.execute('DELETE FROM dados_eventos')
cursor.execute('DELETE FROM outros_dados_eventos')

conn.commit()

for index, row in ler_arquivo_csv.iterrows():

    cursor.execute('''
        INSERT INTO eventos (nome, tipo)
        VALUES (?, ?)
    ''', (row['nome_evento'], row['tipo_evento']))

    id_evento = cursor.lastrowid

    cursor.execute('''
        INSERT INTO dados_eventos (id_evento, data, localizacao)
        VALUES (?, ?, ?)
    ''', (id_evento, row['data_evento'], row['local_evento']))

    cursor.execute('''
    INSERT INTO outros_dados_eventos (id_evento, metadado, ambiente)
    VALUES (?, ?, ?)
    ''', (id_evento, row['link'], row['ambiente']))


conn.commit()

print("Dados adicionados ao banco de dados com sucesso!")


# 1. Mostrar todos os eventos com suas datas, localização e tipo de evento
cursor.execute('''
SELECT e.nome, e.tipo, de.data, de.localizacao
FROM eventos e
JOIN dados_eventos de ON e.id = de.id_evento
''')
eventos = cursor.fetchall()
print("Todos os eventos com suas datas, localização e tipo de evento:")
for evento in eventos:
    print(evento)

# 2. Mostrar os dados dos 2 eventos mais próximos de iniciar
cursor.execute('''
SELECT e.nome, de.data, de.localizacao
FROM eventos e
JOIN dados_eventos de ON e.id = de.id_evento
ORDER BY de.data ASC
LIMIT 2
''')
eventos_proximos = cursor.fetchall()
print("\nDois eventos mais próximos de iniciar:")
for evento in eventos_proximos:
    print(evento)

# 3. Mostrar os eventos que acontecem no Rio de Janeiro
cursor.execute('''
SELECT e.nome, de.data, de.localizacao
FROM eventos e
JOIN dados_eventos de ON e.id = de.id_evento
WHERE de.localizacao LIKE '%Rio de Janeiro%'
''')
eventos_rj = cursor.fetchall()
print("\nEventos que acontecem no Rio de Janeiro:")
for evento in eventos_rj:
    print(evento)

# 4. Mostrar todos os eventos que são ao ar livre
cursor.execute('''
SELECT e.nome, de.data, de.localizacao, ode.ambiente
FROM eventos e
JOIN dados_eventos de ON e.id = de.id_evento
JOIN outros_dados_eventos ode ON e.id = ode.id_evento
WHERE ode.ambiente = 'Ao ar livre'
''')
eventos_ao_ar_livre = cursor.fetchall()
print("\nEventos que são ao ar livre:")
for evento in eventos_ao_ar_livre:
    print(evento)

# 5. Mostrar todos os Metadados por evento
cursor.execute('''
SELECT e.nome, ode.metadado, ode.ambiente
FROM eventos e
JOIN outros_dados_eventos ode ON e.id = ode.id_evento
''')
links_eventos = cursor.fetchall()
print("\nLinks e Metadados dos eventos:")
for evento in links_eventos:
    print(evento)

conn.close()

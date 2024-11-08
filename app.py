from flask import Flask, jsonify, request
import pandas as pd
import psycopg2
import os

app = Flask(__name__)

# Função para conectar ao banco de dados
def conectar_banco():
    try:
        conn = psycopg2.connect(
            host="ep-dry-dawn-a5k50ozh.us-east-2.aws.neon.tech",
            dbname="tickets_zendesk",
            user="tickets_zendesk_owner",
            password="ISyDqw4GxaV3",
            port="5432",
            options="-c search_path=dbo"
        )
        return conn
    except Exception as e:
        print("Erro ao conectar ao banco de dados:", e)
        return None

# Função para criar a tabela 'tickets' se não existir
def criar_tabela_se_necessario(conn):
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS tickets (
            ticket_id INTEGER PRIMARY KEY,
            chat_content TEXT,
            timestamp TIMESTAMP,
            image_path TEXT
        )
    """)
    conn.commit()
    cursor.close()

# Endpoint para processar tickets
@app.route('/processar_tickets', methods=['POST'])
def processar_tickets():
    csv_path = request.json.get('csv_path', './comments.csv')
    conn = conectar_banco()
    if not conn:
        return jsonify({"error": "Falha na conexão com o banco de dados"}), 500
    
    criar_tabela_se_necessario(conn)
    erros = []
    
    tickets_inseridos = obter_tickets_inseridos(conn)

    try:
        df = pd.read_csv(csv_path, encoding="utf-8")
        df = df[(df['ticket_id'] >= 176) | (~df['ticket_id'].isin(tickets_inseridos))]
    except Exception as e:
        return jsonify({"error": f"Erro ao ler o arquivo CSV: {e}"}), 500
    
    total = len(df)
    for i, row in df.iterrows():
        ticket_id = row['ticket_id']
        chat_content = row['chat_content']
        timestamp = row['timestamp']
        image_path = row['image_path'] if 'image_path' in row and pd.notna(row['image_path']) else None
        
        try:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO tickets (ticket_id, chat_content, timestamp, image_path)
                VALUES (%s, %s, %s, %s)
            """, (ticket_id, chat_content, timestamp, image_path))
            conn.commit()
        except Exception as e:
            conn.rollback()
            erros.append(f"Erro no ticket {ticket_id}: {e}")
    
    conn.close()
    
    if erros:
        return jsonify({"status": "Processamento concluído com erros", "erros": erros}), 207
    else:
        return jsonify({"status": "Processamento concluído sem erros"}), 200

# Função para verificar tickets já inseridos no banco de dados
def obter_tickets_inseridos(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT ticket_id FROM tickets WHERE ticket_id < 176")
    tickets_inseridos = {row[0] for row in cursor.fetchall()}
    cursor.close()
    return tickets_inseridos

if __name__ == '__main__':
    app.run(debug=True, port=5001)

const express = require('express');
const { Pool } = require('pg');
const cors = require('cors');

const app = express();
const port = 3000;

app.use(cors());

// Configuração da conexão com o PostgreSQL
const pool = new Pool({
    host: process.env.DB_HOST,
    database: process.env.DB_NAME,
    user: process.env.DB_USER,
    password: process.env.DB_PASS,
    port: process.env.DB_PORT,
    ssl: { rejectUnauthorized: false }
});

// Endpoint para consulta de tickets por ticket_id (busca direta)
app.get('/tickets/search/:ticket_id', async (req, res) => {
    const { ticket_id } = req.params;
    try {
        const result = await pool.query(
            `SELECT ticket_id, STRING_AGG(chat_content, '\n' ORDER BY id) AS chat_content
             FROM tickets
             WHERE ticket_id = $1
             GROUP BY ticket_id`,
            [ticket_id]
        );

        if (result.rows.length > 0) {
            res.json(result.rows);
        } else {
            res.status(404).json({ error: 'Ticket não encontrado' });
        }
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// Endpoint para busca genérica (pode buscar por ticket_id, termo de pesquisa ou número de pedido)
app.get('/tickets/search', async (req, res) => {
    const { ticket_id, query, orderNumber } = req.query;

    try {
        let result;
        if (ticket_id) {
            // Busca diretamente pelo ticket_id
            result = await pool.query(
                `SELECT ticket_id, STRING_AGG(chat_content, '\n' ORDER BY id) AS chat_content
                FROM tickets
                WHERE ticket_id = $1
                GROUP BY ticket_id`,
                [ticket_id]
            );
        } else if (orderNumber) {
            // Busca pelo número de pedido no chat_content
            result = await pool.query(
                `SELECT ticket_id, STRING_AGG(chat_content, '\n' ORDER BY id) AS chat_content
                FROM tickets
                WHERE chat_content ILIKE $1
                GROUP BY ticket_id`,
                [`%${orderNumber}%`]
            );
        } else if (query) {
            // Busca por termo de pesquisa (nome, CPF, CNPJ) no chat_content
            result = await pool.query(
                `SELECT ticket_id, STRING_AGG(chat_content, '\n' ORDER BY id) AS chat_content
                FROM tickets
                WHERE chat_content ILIKE $1
                GROUP BY ticket_id`,
                [`%${query}%`]
            );
        }

        if (result && result.rows.length > 0) {
            res.json(result.rows);
        } else {
            res.status(404).json({ error: 'Nenhum ticket encontrado' });
        }
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// Inicia o servidor
app.listen(port, () => {
    console.log(`API rodando em http://localhost:${port}`);
});

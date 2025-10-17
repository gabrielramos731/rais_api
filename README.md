# RAIS API - Versão Simples

API REST minimalista para exportação de dados RAIS em formato Parquet.

## Inicialização

**1. Instale as dependências:**
```bash
pip install fastapi uvicorn sqlalchemy psycopg2-binary pandas pyarrow
```

**2. Configure o banco com o Data Warehouse gerado pelo [ETL RAIS](https://github.com/gabrielramos731/rais-etl):**
```python
DB_CONFIG = {
    'user': 'seu_usuario',
    'password': 'sua_senha',
    'host': 'localhost',
    'port': '5432',
    'database': 'rais'
}
```

**3. Execute:**
```bash
python rais_api.py
```


**4. Acesse a documentação:**
- http://localhost:8000/docs

## Endpoints Disponíveis

### Tabelas Fato

| Endpoint | Descrição |
|----------|-----------|
| `GET /fact/secao-municipio` | Seção × Município |
| `GET /fact/divisao-municipio` | Divisão × Município |
| `GET /fact/secao-microrregiao` | Seção × Microrregião |
| `GET /fact/divisao-microrregiao` | Divisão × Microrregião |
| `GET /fact/secao-mesorregiao` | Seção × Mesorregião |
| `GET /fact/divisao-mesorregiao` | Divisão × Mesorregião |

## Filtros

**Todos os endpoints aceitam:**
- `ano` - Ano da análise
- `uf` ou `id_uf` - Estado
- `municipio` ou `id_municipio` - Município (quando aplicável)
- `microrregiao` ou `id_microrregiao` - Microrregião (quando aplicável)
- `mesorregiao` ou `id_mesorregiao` - Mesorregião (quando aplicável)
- `secao` ou `descricao_secao` - Seção CNAE
- `divisao` ou `descricao_divisao` - Divisão CNAE (quando aplicável)


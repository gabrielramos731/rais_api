"""
RAIS API - Versão Simples
API para consulta e exportação de dados da modelagem dimensional RAIS
"""
from typing import Optional
from fastapi import FastAPI, Depends, Query
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
import io
import pandas as pd


DB_CONFIG = {
    'user': 'postgres',
    'password': '2302',
    'host': 'localhost',
    'port': '5432',
    'database': 'rais'
}

SQLALCHEMY_DATABASE_URL = (
    f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
    f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
)

engine = create_engine(SQLALCHEMY_DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db():
    """Dependency para obter sessão do banco"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


app = FastAPI(
    title="RAIS API",
    description="API para exportação de dados RAIS em formato Parquet",
    version="2.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def build_query(table_name: str, filters: dict) -> tuple[str, dict]:
    """Constrói query SQL com filtros dinâmicos"""
    query = f"SELECT * FROM dimensional.{table_name} WHERE 1=1"
    params = {}
    
    # Ano
    if filters.get('ano'):
        query += " AND ano = :ano"
        params['ano'] = filters['ano']
    
    # UF
    if filters.get('uf'):
        query += " AND UPPER(uf) = UPPER(:uf)"
        params['uf'] = filters['uf']
    elif filters.get('id_uf'):
        query += " AND id_uf = :id_uf"
        params['id_uf'] = filters['id_uf']
    
    # Município
    if filters.get('id_municipio'):
        query += " AND id_municipio = :id_municipio"
        params['id_municipio'] = filters['id_municipio']
    elif filters.get('municipio'):
        query += " AND UPPER(municipio) LIKE UPPER(:municipio)"
        params['municipio'] = f"%{filters['municipio']}%"
    
    # Microrregião
    if filters.get('id_microrregiao'):
        query += " AND id_microrregiao = :id_microrregiao"
        params['id_microrregiao'] = filters['id_microrregiao']
    elif filters.get('microrregiao'):
        query += " AND UPPER(microrregiao) LIKE UPPER(:microrregiao)"
        params['microrregiao'] = f"%{filters['microrregiao']}%"
    
    # Mesorregião
    if filters.get('id_mesorregiao'):
        query += " AND id_mesorregiao = :id_mesorregiao"
        params['id_mesorregiao'] = filters['id_mesorregiao']
    elif filters.get('mesorregiao'):
        query += " AND UPPER(mesorregiao) LIKE UPPER(:mesorregiao)"
        params['mesorregiao'] = f"%{filters['mesorregiao']}%"
    
    # Seção
    if filters.get('secao') is not None:
        query += " AND secao = :secao"
        params['secao'] = filters['secao']
    elif filters.get('descricao_secao'):
        query += " AND UPPER(descricao_secao) LIKE UPPER(:descricao_secao)"
        params['descricao_secao'] = f"%{filters['descricao_secao']}%"
    
    # Divisão
    if filters.get('divisao'):
        query += " AND divisao = :divisao"
        params['divisao'] = filters['divisao']
    elif filters.get('descricao_divisao'):
        query += " AND UPPER(descricao_divisao) LIKE UPPER(:descricao_divisao)"
        params['descricao_divisao'] = f"%{filters['descricao_divisao']}%"
    
    return query, params


def export_to_parquet(db: Session, table_name: str, filename: str, filters: dict):
    """Executa query e retorna arquivo Parquet"""
    try:
        # Constrói e executa query
        query, params = build_query(table_name, filters)
        result = db.execute(text(query), params)
        
        # Converte para DataFrame
        columns = result.keys()
        rows = result.fetchall()
        df = pd.DataFrame(rows, columns=columns)
        
        # Gera Parquet em buffer
        buf = io.BytesIO()
        df.to_parquet(buf, index=False, engine="pyarrow")
        buf.seek(0)
        
        # Retorna StreamingResponse
        return StreamingResponse(
            buf,
            media_type="application/x-parquet",
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"',
                "X-Record-Count": str(len(df))
            }
        )
        
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/", tags=["Status"])
def root():
    """Status da API"""
    return {
        "status": "online",
        "message": "RAIS API",
        "version": "2.0.0",
        "docs": "/docs"
    }


@app.get("/fact/secao-municipio", tags=["Fato"])
def secao_municipio(
    db: Session = Depends(get_db),
    ano: Optional[int] = None,
    uf: Optional[str] = None,
    id_uf: Optional[str] = None,
    id_municipio: Optional[str] = None,
    municipio: Optional[str] = None,
    secao: Optional[int] = None,
    descricao_secao: Optional[str] = None,
    limit: int = Query(10000, gt=0, le=100000)
):
    """Seção CNAE × Município"""
    filters = {k: v for k, v in locals().items() if k not in ['db'] and v is not None}
    return export_to_parquet(db, "fact_sec_muni_mv", "secao_municipio.parquet", filters)


@app.get("/fact/divisao-municipio", tags=["Fato"])
def divisao_municipio(
    db: Session = Depends(get_db),
    ano: Optional[int] = None,
    uf: Optional[str] = None,
    id_uf: Optional[str] = None,
    id_municipio: Optional[str] = None,
    municipio: Optional[str] = None,
    divisao: Optional[str] = None,
    descricao_divisao: Optional[str] = None,
    secao: Optional[int] = None,
    descricao_secao: Optional[str] = None,
    limit: int = Query(10000, gt=0, le=100000)
):
    """Divisão CNAE × Município"""
    filters = {k: v for k, v in locals().items() if k not in ['db'] and v is not None}
    return export_to_parquet(db, "fact_div_muni_mv", "divisao_municipio.parquet", filters)


@app.get("/fact/secao-microrregiao", tags=["Fato"])
def secao_microrregiao(
    db: Session = Depends(get_db),
    ano: Optional[int] = None,
    uf: Optional[str] = None,
    id_uf: Optional[str] = None,
    id_microrregiao: Optional[str] = None,
    microrregiao: Optional[str] = None,
    secao: Optional[int] = None,
    descricao_secao: Optional[str] = None,
    limit: int = Query(10000, gt=0, le=100000)
):
    """Seção CNAE × Microrregião"""
    filters = {k: v for k, v in locals().items() if k not in ['db'] and v is not None}
    return export_to_parquet(db, "fact_sec_micro_mv", "secao_microrregiao.parquet", filters)


@app.get("/fact/divisao-microrregiao", tags=["Fato"])
def divisao_microrregiao(
    db: Session = Depends(get_db),
    ano: Optional[int] = None,
    uf: Optional[str] = None,
    id_uf: Optional[str] = None,
    id_microrregiao: Optional[str] = None,
    microrregiao: Optional[str] = None,
    divisao: Optional[str] = None,
    descricao_divisao: Optional[str] = None,
    secao: Optional[int] = None,
    descricao_secao: Optional[str] = None,
    limit: int = Query(10000, gt=0, le=100000)
):
    """Divisão CNAE × Microrregião"""
    filters = {k: v for k, v in locals().items() if k not in ['db'] and v is not None}
    return export_to_parquet(db, "fact_div_micro_mv", "divisao_microrregiao.parquet", filters)


@app.get("/fact/secao-mesorregiao", tags=["Fato"])
def secao_mesorregiao(
    db: Session = Depends(get_db),
    ano: Optional[int] = None,
    uf: Optional[str] = None,
    id_uf: Optional[str] = None,
    id_mesorregiao: Optional[str] = None,
    mesorregiao: Optional[str] = None,
    secao: Optional[int] = None,
    descricao_secao: Optional[str] = None,
    limit: int = Query(10000, gt=0, le=100000)
):
    """Seção CNAE × Mesorregião"""
    filters = {k: v for k, v in locals().items() if k not in ['db'] and v is not None}
    return export_to_parquet(db, "fact_sec_meso_mv", "secao_mesorregiao.parquet", filters)


@app.get("/fact/divisao-mesorregiao", tags=["Fato"])
def divisao_mesorregiao(
    db: Session = Depends(get_db),
    ano: Optional[int] = None,
    uf: Optional[str] = None,
    id_uf: Optional[str] = None,
    id_mesorregiao: Optional[str] = None,
    mesorregiao: Optional[str] = None,
    divisao: Optional[str] = None,
    descricao_divisao: Optional[str] = None,
    secao: Optional[int] = None,
    descricao_secao: Optional[str] = None,
    limit: int = Query(10000, gt=0, le=100000)
):
    """Divisão CNAE × Mesorregião"""
    filters = {k: v for k, v in locals().items() if k not in ['db'] and v is not None}
    return export_to_parquet(db, "fact_div_meso_mv", "divisao_mesorregiao.parquet", filters)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

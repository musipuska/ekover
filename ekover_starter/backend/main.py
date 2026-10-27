from pathlib import Path
from datetime import datetime, timezone
import uuid
import pandas as pd
from fastapi import FastAPI, File, HTTPException, Query, UploadFile
from fastapi.middleware.cors import CORSMiddleware

ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = ROOT / 'data' / 'raw'
UPLOAD_DIR = ROOT / 'data' / 'uploads'
RAW_DIR.mkdir(parents=True, exist_ok=True)
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
POTABILITY_COLUMNS = {'ph','Hardness','Solids','Chloramines','Sulfate','Conductivity','Organic_carbon','Trihalomethanes','Turbidity','Potability'}
app = FastAPI(title='ЭКОВЕР API', description='API для загрузки и просмотра данных о качестве воды', version='0.1.0')
app.add_middleware(CORSMiddleware, allow_origins=['http://localhost:5173','http://127.0.0.1:5173','http://localhost:3000','http://127.0.0.1:3000'], allow_credentials=True, allow_methods=['*'], allow_headers=['*'])

def all_csv():
    return sorted(RAW_DIR.glob('*.csv')) + sorted(UPLOAD_DIR.glob('*.csv'))

def get_file(filename=None):
    paths = all_csv()
    if filename:
        matches = [p for p in paths if p.name == filename]
        if not matches:
            raise HTTPException(404, 'Файл не найден.')
        return matches[0]
    if not paths:
        raise HTTPException(404, 'CSV-файлы пока не загружены.')
    if len(paths) > 1:
        raise HTTPException(409, 'Найдено несколько CSV. Передайте параметр filename.')
    return paths[0]

def read_csv(path):
    try:
        return pd.read_csv(path)
    except Exception as exc:
        raise HTTPException(422, f'Не удалось прочитать CSV: {exc}')

def report(path):
    df = read_csv(path)
    if df.empty:
        raise HTTPException(422, 'CSV не содержит строк.')
    if any(not str(c).strip() for c in df.columns) or len(set(df.columns)) != len(df.columns):
        raise HTTPException(422, 'Пустые или повторяющиеся названия столбцов.')
    is_potability = POTABILITY_COLUMNS.issubset(set(df.columns))
    return {'filename': path.name, 'rows': int(len(df)), 'columns_count': int(len(df.columns)), 'columns': [str(c) for c in df.columns], 'missing_values': {str(c): int(n) for c,n in df.isna().sum().items() if n > 0}, 'duplicate_rows': int(df.duplicated().sum()), 'dataset_type': 'water_potability' if is_potability else 'unknown_or_custom', 'validation_note': 'Water Potability: подходит для анализа/классификации, но не содержит парных показаний бюджетного и эталонного датчиков.' if is_potability else 'Пользовательский CSV. Проверьте соответствие согласованной схеме ML.'}

@app.get('/')
def home(): return {'project':'ЭКОВЕР','message':'API работает','docs':'/docs'}
@app.get('/health')
def health(): return {'status':'ok'}
@app.get('/files')
def files():
    items=[]
    for p in all_csv():
        items.append({'filename':p.name,'location':'raw' if p.parent == RAW_DIR else 'uploads','size_bytes':p.stat().st_size,'modified_at':datetime.fromtimestamp(p.stat().st_mtime,tz=timezone.utc).isoformat()})
    return {'items':items,'total':len(items)}
@app.post('/files/upload')
async def upload_csv(file: UploadFile = File(...)):
    if not file.filename or not file.filename.lower().endswith('.csv'):
        raise HTTPException(415,'Загрузите файл с расширением .csv.')
    content=await file.read(25*1024*1024+1)
    if len(content)>25*1024*1024: raise HTTPException(413,'Размер файла превышает 25 МБ.')
    if not content: raise HTTPException(422,'Загружен пустой файл.')
    safe_name=Path(file.filename).name
    target=UPLOAD_DIR/f'{uuid.uuid4().hex[:8]}_{safe_name}'
    target.write_bytes(content)
    try: result=report(target)
    except Exception:
        target.unlink(missing_ok=True); raise
    return {'message':'CSV загружен и прошёл базовую проверку.','file':result,'stored_filename':target.name,'location':'data/uploads'}
@app.get('/dataset/info')
def dataset_info(filename: str | None = None): return report(get_file(filename))
@app.get('/measurements')
def measurements(filename: str | None = None, limit: int = Query(10,ge=1,le=100), offset: int = Query(0,ge=0)):
    path=get_file(filename); df=read_csv(path); page=df.iloc[offset:offset+limit].astype(object); page=page.where(pd.notna(page),None)
    return {'filename':path.name,'total':int(len(df)),'limit':limit,'offset':offset,'items':page.to_dict(orient='records')}
@app.post('/analysis/run')
def analysis_run(filename: str | None = None):
    path=get_file(filename); details=report(path)
    return {'status':'not_connected','message':'Временная заглушка: ML-сервис ещё не подключён.','filename':path.name,'rows_received':details['rows'],'next_step':'Согласовать с ML-разработчиком URL сервиса, формат запроса и формат ответа.'}

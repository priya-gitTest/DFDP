from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse
from rdflib import Graph, Namespace, RDF
from fastapi.templating import Jinja2Templates
from pathlib import Path

from fastapi.responses import JSONResponse, PlainTextResponse
from rdflib.plugins.sparql.processor import prepareQuery
from rdflib.query import Result

app = FastAPI(title="DICOM Catalog API", description="DICOM files grouped by modality")
templates = Jinja2Templates(directory="templates")

# Load DCAT RDF file
#RDF_FILE = "dcat_catalog_20250807_134724.ttl"  # Update with actual file
RDF_FILE = "dcat_catalog_20250807_143818.ttl"  # Update with actual file
g = Graph()
g.parse(RDF_FILE, format="turtle")

# Namespaces
DCAT = Namespace("http://www.w3.org/ns/dcat#")
DCT = Namespace("http://purl.org/dc/terms/")
FOAF = Namespace("http://xmlns.com/foaf/0.1/")

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse("datasets.html", {"request": request, "datasets": list_datasets()})

@app.get("/catalog")
async def catalog():
    catalog_uri = next(g.subjects(RDF.type, DCAT.Catalog), None)
    if catalog_uri:
        return {
            "uri": str(catalog_uri),
            "title": str(g.value(catalog_uri, DCT.title)),
            "description": str(g.value(catalog_uri, DCT.description)),
            "issued": str(g.value(catalog_uri, DCT.issued)),
            "creator": str(g.value(catalog_uri, DCT.creator)),
        }
    return {"error": "No catalog found"}

@app.get("/datasets")
async def get_datasets():
    return list_datasets()

@app.get("/dataset/{dataset_id}")
async def get_dataset(dataset_id: str):
    for s in g.subjects(RDF.type, DCAT.Dataset):
        if dataset_id in str(s):
            return {
                "uri": str(s),
                "title": str(g.value(s, DCT.title)),
                "description": str(g.value(s, DCT.description)),
                "date": str(g.value(s, DCT.date)),
                "identifier": str(g.value(s, DCT.identifier)),
                "creator": str(g.value(s, DCT.creator)),
                "patient_name": str(g.value(s, FOAF.name))
            }
    return {"error": "Dataset not found"}

@app.post("/sparql")
async def sparql_query(query: str = Form(...), output: str = Form("json")):
    try:
        q = prepareQuery(query)
        results = g.query(q)

        if output == "json":
            return JSONResponse(results_to_json(results))
        elif output == "csv":
            return PlainTextResponse(results.serialize(format="csv"))
        elif output == "xml":
            return PlainTextResponse(results.serialize(format="xml"))
        else:
            return PlainTextResponse(results.serialize(format="txt"))
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)
    
@app.get("/sparql-ui", response_class=HTMLResponse)
async def sparql_form(request: Request):
    return templates.TemplateResponse("sparql.html", {"request": request})

@app.post("/sparql-ui", response_class=HTMLResponse)
async def sparql_form_post(request: Request, query: str = Form(...)):
    try:
        q = prepareQuery(query)
        results = g.query(q)
        results_json = results_to_json(results)
        return templates.TemplateResponse("sparql.html", {"request": request, "query": query, "results": results_json})
    except Exception as e:
        return templates.TemplateResponse("sparql.html", {"request": request, "query": query, "error": str(e)})

# Helper function
def list_datasets():
    datasets = []
    for s in g.subjects(RDF.type, DCAT.Dataset):
        datasets.append({
            "id": str(s).split("/")[-1],
            "title": str(g.value(s, DCT.title)),
            "identifier": str(g.value(s, DCT.identifier)),
            "modality": str(g.value(s, DCT.description)),
            "date": str(g.value(s, DCT.date)),
        })
    return datasets

def results_to_json(results: Result):
    json_results = []
    for row in results:
        json_results.append({str(var): str(val) for var, val in zip(results.vars, row)})
    return json_results


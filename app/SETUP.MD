pip install uvicorn
pip install python-multipart
pip install fastapi rdflib jinja2 or pip install -r requirements.txt

uvicorn main:app --reload

lsof -i :8000
kill -9 27972

https://silver-space-robot-r4jqj5xqq972554p-8000.app.github.dev/sparql-ui

SELECT ?dataset ?title WHERE {
  ?dataset a <http://www.w3.org/ns/dcat#Dataset> ;
           <http://purl.org/dc/terms/title> ?title .
}

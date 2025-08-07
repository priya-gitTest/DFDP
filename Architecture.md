              ┌──────────────────────┐
              │  The Cancer Imaging  │
              │      Archive (TCIA)  │
              └──────────┬───────────┘
                         ▼
             ┌─────────────────────┐
             │ Download DICOM files│
             └──────────┬──────────┘
                        ▼
            ┌──────────────────────┐
            │ Read DICOM metadata  │ ←─ using `pydicom`
            └──────────┬───────────┘
                       ▼
       ┌──────────────────────────────────┐
       │ Map metadata to Ontology & DCAT  │ ←─ using `rdflib`
       └────────────────┬─────────────────┘
                        ▼
        ┌────────────────────────────────┐
        │ Generate DCAT Turtle catalog   │
        └────────────────┬───────────────┘
                         ▼
        ┌────────────────────────────────┐
        │   Serve via FastAPI + SPARQL   │ ←─ with `/datasets` & `/sparql`
        └────────────────────────────────┘

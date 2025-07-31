"""
DICOM to FAIR Data Point Integration
====================================

This script demonstrates how to extract DICOM metadata and insert it into a 
FAIR Data Point to create a federated data catalog following DCAT-AP standards.

Requirements:
- pydicom: For DICOM file parsing
- rdflib: For RDF/Turtle generation
- requests: For FDP API communication
- python-dateutil: For date parsing
"""

import os
import json
import requests
from datetime import datetime
from typing import Dict, List, Optional, Any
import pydicom
from rdflib import Graph, Namespace, URIRef, Literal, BNode
from rdflib.namespace import RDF, RDFS, DCTERMS, XSD
import hashlib
import uuid
from pathlib import Path

# Define namespaces
DCAT = Namespace("http://www.w3.org/ns/dcat#")
FOAF = Namespace("http://xmlns.com/foaf/0.1/")
VCARD = Namespace("http://www.w3.org/2006/vcard/ns#")
ROO = Namespace("http://purl.org/roo/ontology#")
FDP = Namespace("https://w3id.org/fdp/fdp-o#")

class DICOMMetadataExtractor:
    """Extract and normalize DICOM metadata for FAIR Data Point integration."""
    
    def __init__(self):
        self.dicom_to_roo_mapping = {
            'PatientName': 'roo:hasPatientName',
            'PatientID': 'roo:hasPatientID',
            'PatientBirthDate': 'roo:hasBirthDate',
            'PatientSex': 'roo:hasGender',
            'StudyDescription': 'roo:hasStudyDescription',
            'SeriesDescription': 'roo:hasSeriesDescription',
            'StudyDate': 'roo:hasStudyDate',
            'SeriesDate': 'roo:hasSeriesDate',
            'Modality': 'roo:hasModality'
        }
    
    def extract_dicom_metadata(self, dicom_file_path: str) -> Dict[str, Any]:
        """Extract relevant metadata from DICOM file."""
        try:
            ds = pydicom.dcmread(dicom_file_path)
            
            metadata = {
                'file_path': dicom_file_path,
                'file_size': os.path.getsize(dicom_file_path),
                'sop_instance_uid': str(getattr(ds, 'SOPInstanceUID', '')),
                'study_instance_uid': str(getattr(ds, 'StudyInstanceUID', '')),
                'series_instance_uid': str(getattr(ds, 'SeriesInstanceUID', '')),
                'patient_name': str(getattr(ds, 'PatientName', '')),
                'patient_id': str(getattr(ds, 'PatientID', '')),
                'patient_birth_date': str(getattr(ds, 'PatientBirthDate', '')),
                'patient_sex': str(getattr(ds, 'PatientSex', '')),
                'study_description': str(getattr(ds, 'StudyDescription', '')),
                'series_description': str(getattr(ds, 'SeriesDescription', '')),
                'study_date': str(getattr(ds, 'StudyDate', '')),
                'series_date': str(getattr(ds, 'SeriesDate', '')),
                'modality': str(getattr(ds, 'Modality', '')),
                'institution_name': str(getattr(ds, 'InstitutionName', '')),
                'manufacturer': str(getattr(ds, 'Manufacturer', '')),
                'manufacturer_model': str(getattr(ds, 'ManufacturerModelName', ''))
            }
            
            return metadata
        
        except Exception as e:
            print(f"Error reading DICOM file {dicom_file_path}: {e}")
            return None

    def format_date(self, dicom_date: str) -> Optional[str]:
        """Convert DICOM date format (YYYYMMDD) to ISO format."""
        if not dicom_date or len(dicom_date) < 8:
            return None
        try:
            date_obj = datetime.strptime(dicom_date[:8], '%Y%m%d')
            return date_obj.isoformat().split('T')[0]
        except ValueError:
            return None

class FairDataPointClient:
    """Client for interacting with FAIR Data Point API."""
    
    def __init__(self, fdp_base_url: str, api_key: Optional[str] = None):
        self.base_url = fdp_base_url.rstrip('/')
        self.api_key = api_key
        self.session = requests.Session()
        
        if api_key:
            self.session.headers.update({
                'Authorization': f'Bearer {api_key}',
                'Content-Type': 'application/json'
            })
    
    def create_catalog(self, catalog_data: Dict[str, Any]) -> Optional[str]:
        """Create a new catalog in FDP."""
        try:
            response = self.session.post(
                f'{self.base_url}/catalogs',
                json=catalog_data
            )
            response.raise_for_status()
            return response.json().get('uri')
        except requests.RequestException as e:
            print(f"Error creating catalog: {e}")
            return None
    
    def create_dataset(self, catalog_uri: str, dataset_data: Dict[str, Any]) -> Optional[str]:
        """Create a new dataset in the specified catalog."""
        try:
            response = self.session.post(
                f'{self.base_url}/datasets',
                json=dataset_data
            )
            response.raise_for_status()
            return response.json().get('uri')
        except requests.RequestException as e:
            print(f"Error creating dataset: {e}")
            return None
    
    def create_distribution(self, dataset_uri: str, distribution_data: Dict[str, Any]) -> Optional[str]:
        """Create a new distribution for the specified dataset."""
        try:
            response = self.session.post(
                f'{self.base_url}/distributions',
                json=distribution_data
            )
            response.raise_for_status()
            return response.json().get('uri')
        except requests.RequestException as e:
            print(f"Error creating distribution: {e}")
            return None

class DICOMFDPIntegrator:
    """Main class for integrating DICOM datasets with FAIR Data Point."""
    
    def __init__(self, fdp_client: FairDataPointClient):
        self.fdp_client = fdp_client
        self.metadata_extractor = DICOMMetadataExtractor()
        self.graph = Graph()
        self._bind_namespaces()
    
    def _bind_namespaces(self):
        """Bind common namespaces to the RDF graph."""
        self.graph.bind('dcat', DCAT)
        self.graph.bind('dct', DCTERMS)
        self.graph.bind('foaf', FOAF)
        self.graph.bind('vcard', VCARD)
        self.graph.bind('roo', ROO)
        self.graph.bind('fdp', FDP)
    
    def generate_catalog_metadata(self, 
                                title: str,
                                description: str,
                                publisher: str,
                                base_uri: str) -> Dict[str, Any]:
        """Generate DCAT-AP compliant catalog metadata."""
        catalog_uri = f"{base_uri}/catalogs/{uuid.uuid4()}"
        
        catalog_data = {
            "@context": {
                "dcat": "http://www.w3.org/ns/dcat#",
                "dct": "http://purl.org/dc/terms/",
                "foaf": "http://xmlns.com/foaf/0.1/",
                "roo": "http://purl.org/roo/ontology#"
            },
            "@id": catalog_uri,
            "@type": "dcat:Catalog",
            "dct:title": title,
            "dct:description": description,
            "dct:publisher": {
                "@type": "foaf:Organization",
                "foaf:name": publisher
            },
            "dct:language": "en",
            "dct:license": "https://creativecommons.org/licenses/by/4.0/",
            "dcat:themeTaxonomy": "http://purl.org/roo/themes",
            "dct:conformsTo": "http://purl.org/roo/ontology",
            "dct:issued": datetime.now().isoformat(),
            "dct:modified": datetime.now().isoformat()
        }
        
        return catalog_data
    
    def generate_dataset_metadata(self, 
                                dicom_metadata_list: List[Dict[str, Any]],
                                dataset_title: str,
                                dataset_description: str,
                                base_uri: str) -> Dict[str, Any]:
        """Generate DCAT-AP compliant dataset metadata from DICOM metadata."""
        dataset_uri = f"{base_uri}/datasets/{uuid.uuid4()}"
        
        # Extract common metadata from DICOM files
        modalities = set()
        study_descriptions = set()
        patient_count = len(set(item['patient_id'] for item in dicom_metadata_list if item['patient_id']))
        
        for metadata in dicom_metadata_list:
            if metadata['modality']:
                modalities.add(metadata['modality'])
            if metadata['study_description']:
                study_descriptions.add(metadata['study_description'])
        
        # Generate keywords from DICOM metadata
        keywords = list(modalities) + list(study_descriptions) + ["DICOM", "Medical Imaging", "Radiation Oncology"]
        
        dataset_data = {
            "@context": {
                "dcat": "http://www.w3.org/ns/dcat#",
                "dct": "http://purl.org/dc/terms/",
                "foaf": "http://xmlns.com/foaf/0.1/",
                "roo": "http://purl.org/roo/ontology#"
            },
            "@id": dataset_uri,
            "@type": "dcat:Dataset",
            "dct:title": dataset_title,
            "dct:description": f"{dataset_description}. Contains {len(dicom_metadata_list)} DICOM files from {patient_count} patients.",
            "dcat:keyword": keywords,
            "dct:subject": "Medical Imaging",
            "dcat:theme": "http://purl.org/roo/themes/radiation-oncology",
            "dct:conformsTo": "http://purl.org/roo/ontology",
            "dct:language": "en",
            "dct:license": "https://creativecommons.org/licenses/by-nc/4.0/",
            "dct:accessRights": "http://purl.org/coar/access_right/c_16ec",  # restricted access
            "dct:issued": datetime.now().isoformat(),
            "dct:modified": datetime.now().isoformat(),
            "roo:hasModalityCount": len(modalities),
            "roo:hasPatientCount": patient_count,
            "roo:hasFileCount": len(dicom_metadata_list)
        }
        
        return dataset_data
    
    def generate_distribution_metadata(self, 
                                     dicom_metadata_list: List[Dict[str, Any]],
                                     access_url: str,
                                     base_uri: str) -> List[Dict[str, Any]]:
        """Generate distribution metadata for DICOM files."""
        distributions = []
        
        # Create DICOM distribution
        dicom_dist_uri = f"{base_uri}/distributions/{uuid.uuid4()}"
        total_size = sum(item['file_size'] for item in dicom_metadata_list)
        
        dicom_distribution = {
            "@context": {
                "dcat": "http://www.w3.org/ns/dcat#",
                "dct": "http://purl.org/dc/terms/"
            },
            "@id": dicom_dist_uri,
            "@type": "dcat:Distribution",
            "dct:title": "DICOM Files",
            "dct:description": "Original DICOM files with medical imaging data",
            "dcat:accessURL": access_url,
            "dcat:mediaType": "application/dicom",
            "dct:format": "DICOM",
            "dcat:byteSize": total_size,
            "dct:license": "https://creativecommons.org/licenses/by-nc/4.0/"
        }
        distributions.append(dicom_distribution)
        
        # Create RDF/Turtle distribution for semantic data
        rdf_dist_uri = f"{base_uri}/distributions/{uuid.uuid4()}"
        rdf_distribution = {
            "@context": {
                "dcat": "http://www.w3.org/ns/dcat#",
                "dct": "http://purl.org/dc/terms/"
            },
            "@id": rdf_dist_uri,
            "@type": "dcat:Distribution",
            "dct:title": "ROO Semantic Metadata",
            "dct:description": "DICOM metadata mapped to ROO ontology in RDF format",
            "dcat:accessURL": f"{access_url}/metadata.ttl",
            "dcat:mediaType": "text/turtle",
            "dct:format": "RDF/Turtle",
            "dct:conformsTo": "http://purl.org/roo/ontology"
        }
        distributions.append(rdf_distribution)
        
        return distributions
    
    def generate_roo_metadata(self, dicom_metadata_list: List[Dict[str, Any]], base_uri: str) -> str:
        """Generate ROO-compliant RDF metadata from DICOM metadata."""
        
        for metadata in dicom_metadata_list:
            if not metadata:
                continue
                
            # Create patient resource
            patient_uri = URIRef(f"{base_uri}/patients/{metadata['patient_id']}")
            self.graph.add((patient_uri, RDF.type, ROO.Patient))
            
            if metadata['patient_name']:
                self.graph.add((patient_uri, ROO.hasPatientName, Literal(metadata['patient_name'])))
            if metadata['patient_id']:
                self.graph.add((patient_uri, ROO.hasPatientID, Literal(metadata['patient_id'])))
            if metadata['patient_sex']:
                self.graph.add((patient_uri, ROO.hasGender, Literal(metadata['patient_sex'])))
            
            # Format and add birth date
            birth_date = self.metadata_extractor.format_date(metadata['patient_birth_date'])
            if birth_date:
                self.graph.add((patient_uri, ROO.hasBirthDate, Literal(birth_date, datatype=XSD.date)))
            
            # Create study resource
            study_uri = URIRef(f"{base_uri}/studies/{metadata['study_instance_uid']}")
            self.graph.add((study_uri, RDF.type, ROO.ImagingStudy))
            self.graph.add((patient_uri, ROO.hasStudy, study_uri))
            
            if metadata['study_description']:
                self.graph.add((study_uri, ROO.hasStudyDescription, Literal(metadata['study_description'])))
            
            study_date = self.metadata_extractor.format_date(metadata['study_date'])
            if study_date:
                self.graph.add((study_uri, ROO.hasStudyDate, Literal(study_date, datatype=XSD.date)))
            
            # Create series resource
            series_uri = URIRef(f"{base_uri}/series/{metadata['series_instance_uid']}")
            self.graph.add((series_uri, RDF.type, ROO.ImagingSeries))
            self.graph.add((study_uri, ROO.hasSeries, series_uri))
            
            if metadata['series_description']:
                self.graph.add((series_uri, ROO.hasSeriesDescription, Literal(metadata['series_description'])))
            if metadata['modality']:
                self.graph.add((series_uri, ROO.hasModality, Literal(metadata['modality'])))
            
            series_date = self.metadata_extractor.format_date(metadata['series_date'])
            if series_date:
                self.graph.add((series_uri, ROO.hasSeriesDate, Literal(series_date, datatype=XSD.date)))
        
        return self.graph.serialize(format='turtle')
    
    def process_dicom_directory(self, 
                              dicom_directory: str,
                              catalog_title: str,
                              dataset_title: str,
                              publisher: str,
                              access_url: str,
                              base_uri: str = "https://example.org/fdp") -> Dict[str, Any]:
        """Process all DICOM files in a directory and create FDP catalog entry."""
        
        print(f"Processing DICOM files in: {dicom_directory}")
        
        # Extract metadata from all DICOM files
        dicom_metadata_list = []
        for root, dirs, files in os.walk(dicom_directory):
            for file in files:
                if file.lower().endswith(('.dcm', '.dicom')):
                    file_path = os.path.join(root, file)
                    metadata = self.metadata_extractor.extract_dicom_metadata(file_path)
                    if metadata:
                        dicom_metadata_list.append(metadata)
        
        if not dicom_metadata_list:
            print("No valid DICOM files found!")
            return None
        
        print(f"Found {len(dicom_metadata_list)} DICOM files")
        
        # Generate catalog metadata
        catalog_data = self.generate_catalog_metadata(
            title=catalog_title,
            description=f"FAIR Data Point catalog for {catalog_title}",
            publisher=publisher,
            base_uri=base_uri
        )
        
        # Generate dataset metadata
        dataset_data = self.generate_dataset_metadata(
            dicom_metadata_list=dicom_metadata_list,
            dataset_title=dataset_title,
            dataset_description=f"DICOM dataset containing medical imaging data",
            base_uri=base_uri
        )
        
        # Generate distribution metadata
        distributions = self.generate_distribution_metadata(
            dicom_metadata_list=dicom_metadata_list,
            access_url=access_url,
            base_uri=base_uri
        )
        
        # Generate ROO semantic metadata
        roo_metadata = self.generate_roo_metadata(dicom_metadata_list, base_uri)
        
        return {
            'catalog': catalog_data,
            'dataset': dataset_data,
            'distributions': distributions,
            'roo_metadata': roo_metadata,
            'summary': {
                'total_files': len(dicom_metadata_list),
                'unique_patients': len(set(item['patient_id'] for item in dicom_metadata_list if item['patient_id'])),
                'modalities': list(set(item['modality'] for item in dicom_metadata_list if item['modality'])),
                'total_size_mb': sum(item['file_size'] for item in dicom_metadata_list) / (1024 * 1024)
            }
        }
    
    def upload_to_fdp(self, fdp_data: Dict[str, Any]) -> Dict[str, str]:
        """Upload the generated metadata to FAIR Data Point."""
        results = {}
        
        # Create catalog
        print("Creating catalog...")
        catalog_uri = self.fdp_client.create_catalog(fdp_data['catalog'])
        if catalog_uri:
            results['catalog_uri'] = catalog_uri
            print(f"Catalog created: {catalog_uri}")
        else:
            print("Failed to create catalog")
            return results
        
        # Create dataset
        print("Creating dataset...")
        dataset_uri = self.fdp_client.create_dataset(catalog_uri, fdp_data['dataset'])
        if dataset_uri:
            results['dataset_uri'] = dataset_uri
            print(f"Dataset created: {dataset_uri}")
        else:
            print("Failed to create dataset")
            return results
        
        # Create distributions
        print("Creating distributions...")
        distribution_uris = []
        for dist_data in fdp_data['distributions']:
            dist_uri = self.fdp_client.create_distribution(dataset_uri, dist_data)
            if dist_uri:
                distribution_uris.append(dist_uri)
                print(f"Distribution created: {dist_uri}")
        
        results['distribution_uris'] = distribution_uris
        
        return results

def main():
    """Example usage of the DICOM to FDP integration."""
    
    # Configuration
    FDP_BASE_URL = "https://fdp.example.org"  # Replace with your FDP instance
    API_KEY = "your-api-key-here"  # Replace with your API key
    DICOM_DIRECTORY = "/dicom_files"  # Replace with your DICOM directory
    ACCESS_URL = "https://data.example.org/dicom-dataset"  # Replace with actual access URL
    
    # Initialize components
    fdp_client = FairDataPointClient(FDP_BASE_URL, API_KEY)
    integrator = DICOMFDPIntegrator(fdp_client)
    
    # Process DICOM directory and generate metadata
    fdp_data = integrator.process_dicom_directory(
        dicom_directory=DICOM_DIRECTORY,
        catalog_title="Radiation Oncology Clinical Data Catalog",
        dataset_title="Multi-Modal Treatment Planning Dataset",
        publisher="Medical Research Institute",
        access_url=ACCESS_URL
    )
    
    if fdp_data:
        print("\n=== Processing Summary ===")
        print(f"Total files: {fdp_data['summary']['total_files']}")
        print(f"Unique patients: {fdp_data['summary']['unique_patients']}")
        print(f"Modalities: {', '.join(fdp_data['summary']['modalities'])}")
        print(f"Total size: {fdp_data['summary']['total_size_mb']:.2f} MB")
        
        # Save generated metadata locally
        os.makedirs("output", exist_ok=True)
        
        with open("output/catalog.json", "w") as f:
            json.dump(fdp_data['catalog'], f, indent=2)
        
        with open("output/dataset.json", "w") as f:
            json.dump(fdp_data['dataset'], f, indent=2)
        
        with open("output/metadata.ttl", "w") as f:
            f.write(fdp_data['roo_metadata'])
        
        print("\nMetadata files saved to 'output' directory")
        
        # Upload to FDP (uncomment to actually upload)
        # print("\nUploading to FAIR Data Point...")
        # results = integrator.upload_to_fdp(fdp_data)
        # print("Upload results:", results)
    
    else:
        print("Failed to process DICOM directory")

if __name__ == "__main__":
    main()

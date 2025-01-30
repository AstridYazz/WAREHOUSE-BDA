import kaggle

# Descargar el dataset
kaggle.api.dataset_download_files('mexwell/carrier-on-time-performance-dataset', path='./data', unzip=True)

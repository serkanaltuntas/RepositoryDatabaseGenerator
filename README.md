
1. **Collect from all repositories**:
```bash
python package_registry_scraper.py
```

2. **Collect from specific repositories**:
```bash
# Single repository
python package_registry_scraper.py --repos conda-forge

# Multiple repositories
python package_registry_scraper.py --repos conda-forge pypi npm
```

3. **List available repositories**:
```bash
python package_registry_scraper.py --list-repos
```

4. **Specify output file**:
```bash
python package_registry_scraper.py --repos pypi --output pypi_packages.json
```

5. **Retry**:
```bash
# Run with default settings
python package_registry_scraper.py -r pypi

# Run with periodic saving
python package_registry_scraper.py -r pypi --save-interval 300
```

6. **suppress**:
```bash
python package_registry_scraper.py --repos pypi --suppress-not-found
```



python package_registry_scraper.py -r conda-forge pypi bioconductor --output-dir ./data --save-interval 100

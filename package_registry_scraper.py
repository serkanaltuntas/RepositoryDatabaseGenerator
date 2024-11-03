#!/usr/bin/env python3
import sys
import time
import json
import logging
import argparse
import textwrap
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, Set
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from urllib.parse import urljoin
from multiprocessing import Manager
from queue import Empty, Queue
from pathlib import Path
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm


# Configure logging
logging.basicConfig(
    level=logging.ERROR,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class PackageInfo:
    """Data class to store package information"""
    name: str
    versions: Optional[List[str]] = None
    repository: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class PackageRepositoryScraper(ABC):
    """Abstract base class for package repository scrapers with improved concurrency"""

    def __init__(self, repository_name: str, max_workers: int = 10, rate_limit: float = 0.1, chunk_size: int = 100):
        self.repository_name = repository_name
        self.max_workers = max_workers
        self.rate_limit = rate_limit
        self.chunk_size = chunk_size
        self.packages_data: List[PackageInfo] = []

    def create_session(self) -> requests.Session:
        """Create a new session for each process"""
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'PackageRegistryCollector/1.0',
            'Accept': 'application/json',
        })
        return session

    def process_package_chunk(self, package_chunk: List[str], progress_queue: Queue,
                              suppress_not_found: bool = False) -> List[PackageInfo]:
        """Process a chunk of packages with a dedicated session and report progress"""
        session = self.create_session()
        results = []

        for package_name in package_chunk:
            try:
                versions = self.get_package_versions(package_name, session)
                package_info = PackageInfo(
                    name=package_name,
                    versions=versions if versions else None,
                    repository=self.repository_name
                )
                results.append(package_info)
                progress_queue.put((self.repository_name, 1))  # Report progress for each package
                time.sleep(self.rate_limit)
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 404 and suppress_not_found:
                    results.append(PackageInfo(name=package_name, repository=self.repository_name))
                    progress_queue.put((self.repository_name, 1))
            except Exception as e:
                logger.error(f"Error processing package {package_name}: {str(e)}")
                progress_queue.put((self.repository_name, 1))  # Still report progress even on error

        return results

    def collect_all_data(self, suppress_not_found: bool = False, progress_queue: Queue = None):
        """Collect data with progress reporting"""
        session = self.create_session()
        package_names = self.get_all_packages(session)

        if not package_names:
            if progress_queue:
                progress_queue.put((self.repository_name, -1))  # Signal completion
            return

        # Split packages into chunks
        package_chunks = [
            package_names[i:i + self.chunk_size]
            for i in range(0, len(package_names), self.chunk_size)
        ]

        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all chunks for processing
            futures = [
                executor.submit(self.process_package_chunk, chunk, progress_queue, suppress_not_found)
                for chunk in package_chunks
            ]

            # Process completed chunks
            for future in as_completed(futures):
                try:
                    chunk_results = future.result()
                    self.packages_data.extend(chunk_results)
                except Exception as e:
                    logger.error(f"Error processing chunk: {str(e)}")

        # Signal completion
        if progress_queue:
            progress_queue.put((self.repository_name, -1))

    @abstractmethod
    def get_package_versions(self, package_name: str, session: requests.Session) -> List[str]:
        """Get all versions for a package"""
        pass

    @abstractmethod
    def get_all_packages(self, session: requests.Session) -> List[str]:
        """Get list of all packages"""
        pass

    @abstractmethod
    def retry_request(self, url: str, session: requests.Session, params: Optional[Dict[str, Any]] = None,
                      retries: int = 3) -> Optional[requests.Response]:
        """Make HTTP request with retries"""
        pass


class CondaForgeScraper(PackageRepositoryScraper):
    def __init__(self):
        super().__init__("conda-forge", max_workers=10, rate_limit=0.1, chunk_size=100)
        self.base_url = "https://api.anaconda.org/package/conda-forge/"
        self.search_url = "https://conda.anaconda.org/conda-forge/channeldata.json"

    def get_package_versions(self, package_name: str, session: requests.Session) -> List[str]:
        response = self.retry_request(f"{self.base_url}{package_name}", session=session)
        return sorted(response.json().get('versions', []), reverse=True) if response else []

    def get_all_packages(self, session: requests.Session) -> List[str]:
        response = self.retry_request(self.search_url, session=session)
        return sorted(list(response.json().get('packages', {}).keys())) if response else []

    def retry_request(self, url: str, session: requests.Session, params: Optional[Dict[str, Any]] = None,
                      retries: int = 3) -> Optional[requests.Response]:
        for attempt in range(retries):
            try:
                response = session.get(url, params=params)
                response.raise_for_status()
                return response
            except requests.exceptions.RequestException as e:
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
        return None


class PyPIScraper(PackageRepositoryScraper):
    def __init__(self):
        super().__init__("pypi", max_workers=10, rate_limit=0.1, chunk_size=100)
        self.base_url = "https://pypi.org/pypi"
        self.simple_api_url = "https://pypi.org/simple/"

    def get_package_versions(self, package_name: str, session: requests.Session) -> List[str]:
        response = self.retry_request(f"{self.base_url}/{package_name}/json", session=session)
        return sorted(list(response.json()['releases'].keys()), reverse=True) if response else []

    def get_all_packages(self, session: requests.Session) -> List[str]:
        response = self.retry_request(self.simple_api_url, session=session)
        soup = BeautifulSoup(response.text, 'html.parser') if response else None
        return sorted([a.text for a in soup.find_all('a')]) if soup else []

    def retry_request(self, url: str, session: requests.Session, params: Optional[Dict[str, Any]] = None,
                      retries: int = 3) -> Optional[requests.Response]:
        for attempt in range(retries):
            try:
                response = session.get(url, params=params)
                response.raise_for_status()
                return response
            except requests.exceptions.RequestException as e:
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
        return None


class BioconductorScraper(PackageRepositoryScraper):
    def __init__(self):
        super().__init__("bioconductor", max_workers=5, rate_limit=0.2, chunk_size=50)  # More conservative settings
        self.base_url = "https://bioconductor.org/packages/release/bioc"
        self.packages_url = "https://bioconductor.org/packages/release/bioc/VIEWS"
        self.cache = {}  # Simple cache for package versions

    def get_package_versions(self, package_name: str, session: requests.Session) -> List[str]:
        # Check cache first
        if package_name in self.cache:
            return self.cache[package_name]

        try:
            response = self.retry_request(f"{self.base_url}/html/{package_name}.html", session=session)
            if response:
                soup = BeautifulSoup(response.text, 'html.parser')
                version_tag = soup.find('td', string='Version:')
                if version_tag:
                    version_sibling = version_tag.find_next_sibling('td')
                    if version_sibling:
                        version = [version_sibling.text.strip()]
                        self.cache[package_name] = version  # Cache the result
                        return version
        except Exception as e:
            logger.error(f"Error fetching versions for {package_name}: {str(e)}")
        return []

    def get_all_packages(self, session: requests.Session) -> List[str]:
        response = self.retry_request(self.packages_url, session=session)
        return [line.split('Package: ')[1].strip() for line in response.text.splitlines() if
                line.startswith('Package: ')] if response else []

    def retry_request(self, url: str, session: requests.Session, params: Optional[Dict[str, Any]] = None,
                      retries: int = 3) -> Optional[requests.Response]:
        for attempt in range(retries):
            try:
                response = session.get(url, params=params)
                response.raise_for_status()
                return response
            except requests.exceptions.RequestException as e:
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
        return None


class PackageRegistryCollector:
    AVAILABLE_REPOSITORIES = {
        'conda-forge': CondaForgeScraper,
        'pypi': PyPIScraper,
        'bioconductor': BioconductorScraper
    }

    def __init__(self, repositories: Optional[List[str]] = None, output_dir: str = "output"):
        self.selected_repos = repositories or list(self.AVAILABLE_REPOSITORIES.keys())
        self.scrapers = [self.AVAILABLE_REPOSITORIES[repo]() for repo in self.selected_repos]
        self.results = {}
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    def save_progress(self, repo_name: str, data: List[PackageInfo]):
        """Save current progress to a JSON file"""
        output_file = self.output_dir / f"{repo_name}_{self.timestamp}.json"

        # Convert PackageInfo objects to dictionaries
        json_data = [
            {
                "name": pkg.name,
                "versions": pkg.versions,
                "repository": pkg.repository,
                "metadata": pkg.metadata
            }
            for pkg in data
        ]

        try:
            with open(output_file, 'w') as f:
                json.dump(json_data, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving progress for {repo_name}: {str(e)}")

    def collect_all_repositories(self, suppress_not_found: bool = False, save_interval: int = 100):
        # Initialize progress tracking
        manager = Manager()
        progress_queue = manager.Queue()
        save_queue = manager.Queue()  # New queue for save coordination
        progress_bars = {}

        # Create space for progress bars
        print('\n' * (len(self.scrapers) - 1))

        try:
            # Initialize progress bars
            for scraper in self.scrapers:
                session = scraper.create_session()
                package_names = scraper.get_all_packages(session)
                total_packages = len(package_names) if package_names else 0

                progress_bars[scraper.repository_name] = tqdm(
                    total=total_packages,
                    desc=f"{scraper.repository_name:<12}",
                    position=list(self.AVAILABLE_REPOSITORIES.keys()).index(scraper.repository_name),
                    leave=True,
                    ncols=80,
                    bar_format="{desc}|{bar:20}| {percentage:3.0f}%",
                    mininterval=0.1
                )

            # Start collection processes
            with ProcessPoolExecutor(max_workers=len(self.scrapers)) as executor:
                # Submit collection tasks
                futures = {
                    executor.submit(
                        self.collect_repository_data,
                        scraper,
                        suppress_not_found,
                        progress_queue,
                        save_queue,
                        save_interval
                    ): scraper.repository_name
                    for scraper in self.scrapers
                }

                # Monitor progress and handle saves
                completed_repos = set()
                while len(completed_repos) < len(self.scrapers):
                    try:
                        # Check for progress updates
                        repo_name, increment = progress_queue.get(timeout=1)
                        if increment == -1:  # Completion signal
                            completed_repos.add(repo_name)
                            progress_bars[repo_name].close()
                        else:
                            progress_bars[repo_name].update(increment)

                        # Check for save requests
                        try:
                            while True:  # Process all pending saves
                                repo_name, data = save_queue.get_nowait()
                                self.save_progress(repo_name, data)
                        except Empty:
                            pass

                    except Empty:
                        # Check for completed futures
                        done_futures = [f for f in futures if f.done()]
                        for future in done_futures:
                            if future.exception():
                                logger.error(f"Error in {futures[future]}: {future.exception()}")
                        if len(done_futures) == len(futures):
                            break
                        continue

                # Process final results
                for future in as_completed(futures):
                    repo_name = futures[future]
                    try:
                        result = future.result()
                        if result:
                            self.save_progress(repo_name, result)  # Save final results
                    except Exception as e:
                        logger.error(f"Error collecting data from {repo_name}: {str(e)}")

        except KeyboardInterrupt:
            print("\nGracefully shutting down...")
            executor.shutdown(wait=False, cancel_futures=True)
            raise
        finally:
            # Ensure all progress bars are closed
            for bar in progress_bars.values():
                try:
                    bar.close()
                except:
                    pass

    def collect_repository_data(self, scraper: PackageRepositoryScraper, suppress_not_found: bool,
                                progress_queue: Queue, save_queue: Queue, save_interval: int) -> List[PackageInfo]:
        """Collect data for a single repository with periodic saves"""
        session = scraper.create_session()
        package_names = scraper.get_all_packages(session)
        results = []

        for i, package_name in enumerate(package_names):
            try:
                versions = scraper.get_package_versions(package_name, session)
                package_info = PackageInfo(
                    name=package_name,
                    versions=versions if versions else None,
                    repository=scraper.repository_name
                )
                results.append(package_info)
                progress_queue.put((scraper.repository_name, 1))

                # Save progress at intervals
                if (i + 1) % save_interval == 0:
                    save_queue.put((scraper.repository_name, results))

                time.sleep(scraper.rate_limit)
            except Exception as e:
                logger.error(f"Error processing package {package_name}: {str(e)}")
                progress_queue.put((scraper.repository_name, 1))

        progress_queue.put((scraper.repository_name, -1))  # Signal completion
        return results

    def get_summary(self) -> Dict[str, Any]:
        """Generate a summary of collected data"""
        return {
            'timestamp': datetime.now().isoformat(),
            'repositories': {
                repo: {
                    'total_packages': len(scraper.packages_data),
                    'status': 'completed' if scraper.packages_data else 'failed',
                    'output_file': f"{repo}_{self.timestamp}.json"
                }
                for repo, scraper in zip(self.selected_repos, self.scrapers)
            }
        }



def parse_arguments():
    parser = argparse.ArgumentParser(
        description='Collect package information from various package repositories.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent('''
            Available repositories:
              - conda-forge
              - pypi
              - bioconductor

            Example usage:
              python script.py -r conda-forge pypi --suppress-not-found --output-dir ./data
        ''')
    )

    parser.add_argument(
        '-r', '--repos',
        nargs='+',
        help='Specify repositories to collect from (space-separated)',
        choices=PackageRegistryCollector.AVAILABLE_REPOSITORIES.keys(),
        metavar='REPO'
    )

    parser.add_argument(
        '--output-dir',
        type=str,
        default='output',
        help='Directory to save JSON output files (default: output)'
    )

    parser.add_argument(
        '--list-repos',
        action='store_true',
        help='List available repositories and exit'
    )

    parser.add_argument(
        '--suppress-not-found',
        action='store_true',
        help='Suppress warnings for packages not found on the repository'
    )

    parser.add_argument(
        '--save-interval',
        type=int,
        default=100,
        help='Interval of packages after which to save progress to output file'
    )

    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        default='ERROR',
        help='Set the logging level'
    )

    return parser.parse_args()



def main():
    args = parse_arguments()

    # Configure logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    if args.list_repos:
        print("Available repositories:")
        for repo in PackageRegistryCollector.AVAILABLE_REPOSITORIES.keys():
            print(f"  - {repo}")
        sys.exit(0)

    try:
        collector = PackageRegistryCollector(
            repositories=args.repos,
            output_dir=args.output_dir
        )
        collector.collect_all_repositories(
            suppress_not_found=args.suppress_not_found,
            save_interval=args.save_interval
        )

        # Print summary
        summary = collector.get_summary()
        print("\nCollection Summary:")
        for repo, stats in summary['repositories'].items():
            print(f"{repo}: {stats['total_packages']} packages collected ({stats['status']})")
            print(f"Output file: {stats['output_file']}")

    except ValueError as e:
        logger.error(f"Error: {str(e)}")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\nProcess interrupted by user. Exiting...")
        sys.exit(1)
    except Exception as e:
        logger.error(f"An unexpected error occurred: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
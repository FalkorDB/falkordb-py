#!/usr/bin/env python
# Copyright (c) 2024, FalkorDB
# Licensed under the MIT License
"""
Setup script for FalkorDB with embedded support.
Downloads and builds Redis and FalkorDB module when embedded extra is installed.
"""
import os
import sys
import json
import logging
import pathlib
import platform
import shutil
import subprocess
import tarfile
import tempfile
import urllib.request
from setuptools import setup, find_packages
from setuptools.command.install import install
from setuptools.command.develop import develop
from distutils.command.build import build
from distutils.core import Extension

logger = logging.getLogger(__name__)

BASEPATH = os.path.dirname(os.path.abspath(__file__))
REDIS_PATH = os.path.join(BASEPATH, 'redis.submodule')
REDIS_VERSION = os.environ.get('REDIS_VERSION', '7.2.4')
REDIS_URL = f'http://download.redis.io/releases/redis-{REDIS_VERSION}.tar.gz'
FALKORDB_VERSION = os.environ.get('FALKORDB_VERSION', 'v4.2.3')


def download_redis_submodule():
    """Download and extract Redis source code."""
    if pathlib.Path(REDIS_PATH).exists():
        shutil.rmtree(REDIS_PATH)
    
    with tempfile.TemporaryDirectory() as tempdir:
        print(f'Downloading Redis {REDIS_VERSION} from {REDIS_URL}')
        try:
            ftpstream = urllib.request.urlopen(REDIS_URL)
            tf = tarfile.open(fileobj=ftpstream, mode="r|gz")
            
            # Get the first directory name
            first_member = tf.next()
            directory = first_member.name.split('/')[0]
            
            # Reset and extract all
            ftpstream = urllib.request.urlopen(REDIS_URL)
            tf = tarfile.open(fileobj=ftpstream, mode="r|gz")
            
            print(f'Extracting Redis archive to {tempdir}')
            tf.extractall(tempdir)
            
            print(f'Moving {os.path.join(tempdir, directory)} -> redis.submodule')
            shutil.move(os.path.join(tempdir, directory), REDIS_PATH)
            print('Redis source downloaded and extracted successfully')
        except Exception as e:
            print(f'Failed to download Redis: {e}')
            raise


def download_falkordb_module():
    """Download FalkorDB module binary from GitHub releases."""
    machine = platform.machine().lower()
    
    if machine in ['x86_64', 'amd64']:
        module_name = 'falkordb-x64.so'
    elif machine in ['aarch64', 'arm64']:
        module_name = 'falkordb-arm64v8.so'
    else:
        raise Exception(f'Unsupported architecture: {machine}')
    
    falkordb_url = f'https://github.com/FalkorDB/FalkorDB/releases/download/{FALKORDB_VERSION}/{module_name}'
    module_path = os.path.join(BASEPATH, 'falkordb.so')
    
    print(f'Downloading FalkorDB module from {falkordb_url}')
    try:
        urllib.request.urlretrieve(falkordb_url, module_path)
        print(f'FalkorDB module downloaded to {module_path}')
    except Exception as e:
        print(f'Failed to download FalkorDB module: {e}')
        raise


def build_redis():
    """Build Redis from source."""
    if not os.path.exists(REDIS_PATH):
        raise Exception('Redis source not found. Run download_redis_submodule() first.')
    
    print('Building Redis...')
    os.environ['CC'] = 'gcc'
    
    cmd = ['make', 'MALLOC=libc', '-j4']  # Use parallel build
    
    print(f'Running: {" ".join(cmd)} in {REDIS_PATH}')
    try:
        result = subprocess.call(cmd, cwd=REDIS_PATH)
        
        if result != 0:
            raise Exception('Failed to build Redis')
        
        print('Redis built successfully')
    except Exception as e:
        print(f'Error building Redis: {e}')
        raise
    
    # Copy binaries to falkordb/bin
    bin_dir = os.path.join(BASEPATH, 'falkordb', 'bin')
    os.makedirs(bin_dir, exist_ok=True)
    
    for binary in ['redis-server', 'redis-cli']:
        src = os.path.join(REDIS_PATH, 'src', binary)
        dst = os.path.join(bin_dir, binary)
        if os.path.exists(src):
            shutil.copy2(src, dst)
            os.chmod(dst, 0o755)
            print(f'Copied {binary} to {bin_dir}')
        else:
            print(f'Warning: {src} not found')
    
    # Copy FalkorDB module
    falkordb_module = os.path.join(BASEPATH, 'falkordb.so')
    if os.path.exists(falkordb_module):
        dst = os.path.join(bin_dir, 'falkordb.so')
        shutil.copy2(falkordb_module, dst)
        os.chmod(dst, 0o755)
        print(f'Copied falkordb.so to {bin_dir}')
    else:
        print('Warning: falkordb.so not found')


class BuildEmbedded(build):
    """Custom build command that downloads and builds Redis + FalkorDB."""
    
    def run(self):
        # Run original build code
        build.run(self)
        
        # Automatically build embedded binaries when:
        # 1. Building from source (not installing from wheel)
        # 2. User can opt-out with FALKORDB_SKIP_EMBEDDED=1
        
        # Check if user explicitly wants to skip
        skip_embedded = os.environ.get('FALKORDB_SKIP_EMBEDDED', '').lower() in ('1', 'true', 'yes')
        if skip_embedded:
            print('Skipping embedded binaries build (FALKORDB_SKIP_EMBEDDED=1)')
            return
        
        # Always build embedded binaries when building from source
        # This ensures they're available for users who install with [embedded] extra
        print('=' * 80)
        print('Building embedded FalkorDB support...')
        print('This may take a few minutes (downloading and compiling Redis)...')
        print('To skip: set FALKORDB_SKIP_EMBEDDED=1')
        print('=' * 80)
        
        try:
            # Download Redis if not present
            if not os.path.exists(REDIS_PATH):
                print('Downloading Redis...')
                download_redis_submodule()
            
            # Download FalkorDB module if not present
            falkordb_module = os.path.join(BASEPATH, 'falkordb.so')
            if not os.path.exists(falkordb_module):
                print('Downloading FalkorDB module...')
                download_falkordb_module()
            
            # Build Redis
            print('Building Redis...')
            build_redis()
            
            print('=' * 80)
            print('Embedded setup complete!')
            print('Binaries are available in falkordb/bin/')
            print('=' * 80)
        except Exception as e:
            print('=' * 80)
            print(f'Warning: Failed to build embedded binaries: {e}')
            print('The package will still work for non-embedded usage.')
            print('To use embedded mode, you can:')
            print('  1. Install build tools (gcc, make) and try again')
            print('  2. Manually place redis-server and falkordb.so in falkordb/bin/')
            print('=' * 80)


class InstallEmbedded(install):
    """Custom install that triggers embedded build."""
    
    def run(self):
        install.run(self)


class DevelopEmbedded(develop):
    """Custom develop that triggers embedded build."""
    
    def run(self):
        develop.run(self)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    
    # Check if Windows (unsupported for embedded mode)
    if sys.platform in ['win32', 'win64']:
        # Don't fail on Windows, just skip embedded build
        # The package will still work for non-embedded usage
        os.environ['FALKORDB_SKIP_EMBEDDED'] = '1'
        print('Note: Embedded mode is not supported on Windows')
        print('Package will be installed without embedded binaries')
    
    # Read requirements from pyproject.toml
    with open('pyproject.toml', 'r') as f:
        import re
        content = f.read()
        # Extract dependencies - simple parsing
        deps_match = re.search(r'\[tool\.poetry\.dependencies\](.*?)\n\n', content, re.DOTALL)
        
    setup(
        name='falkordb',
        version='1.2.0',
        description='Python client for interacting with FalkorDB database',
        author='FalkorDB inc',
        author_email='info@falkordb.com',
        url='http://github.com/falkorDB/falkordb-py',
        packages=find_packages(),
        package_data={
            'falkordb': ['bin/redis-server', 'bin/redis-cli', 'bin/falkordb.so'],
        },
        include_package_data=True,
        install_requires=[
            'redis>=6.0.0,<7.0.0',
            'python-dateutil>=2.9.0',
        ],
        extras_require={
            'embedded': ['psutil>=5.9.0'],
        },
        cmdclass={
            'build': BuildEmbedded,
            'install': InstallEmbedded,
            'develop': DevelopEmbedded,
        },
        python_requires='>=3.8',
        classifiers=[
            'Development Status :: 5 - Production/Stable',
            'Intended Audience :: Developers',
            'License :: OSI Approved :: MIT License',
            'Programming Language :: Python :: 3',
            'Programming Language :: Python :: 3.8',
            'Programming Language :: Python :: 3.9',
            'Programming Language :: Python :: 3.10',
            'Programming Language :: Python :: 3.11',
            'Programming Language :: Python :: 3.12',
        ],
    )

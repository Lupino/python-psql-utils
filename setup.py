from setuptools import setup

# Define runtime dependencies
# Psycopg 3 with binary drivers and connection pool support
REQUIRES = [
    'psycopg[binary,pool]',
]

setup(
    name='psql_utils',
    version='0.1.0',
    description='PostgreSQL Simple util tools',
    author='Li Meng Jun',
    author_email='lmjubuntu@gmail.com',
    url='https://github.com/Lupino/python-psql-utils',
    packages=['psql_utils'],
    # Include the marker file for PEP 561 (Type Hinting) compliance
    package_data={"psql_utils": ["py.typed"]},
    # explicit package mapping
    package_dir={'psql_utils': 'psql_utils'},
    include_package_data=True,
    install_requires=REQUIRES,
    # Minimum Python version (aligned with pyproject.toml)
    python_requires='>=3.8',
    # Classifiers metadata for PyPI indexing
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Topic :: Database',
    ],
)

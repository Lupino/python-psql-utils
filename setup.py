try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

packages = ['psql_utils']

requires = ['psycopg[binary,pool]', 'mypy_extensions']

setup(
    name='psql_utils',
    version='0.1.0',
    description='PostgreSQL Simple util tools',
    author='Li Meng Jun',
    author_email='lmjubuntu@gmail.com',
    url='https://github.com/Lupino/python-psql-utils',
    packages=packages,
    package_data={"psql_utils": ["py.typed"]},
    package_dir={'psql_utils': 'psql_utils'},
    include_package_data=True,
    install_requires=requires,
)

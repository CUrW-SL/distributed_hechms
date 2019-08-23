from setuptools import setup,find_packages

setup(
    name='hechmsd',
    version='1.0.0',
    packages=find_packages(),
    url='http://www.curwsl.org/',
    license='',
    author='hasitha',
    author_email='hasithadkr7@gmail.com',
    description='HecHms Distributed version',
    include_package_data=True,
    install_requires=['FLASK', 'Flask-Uploads', 'Flask-JSON', 'pandas','numpy','shapely', 'joblib', 'netCDF4', 'matplotlib', 'imageio', 'scipy', 'geopandas'],
    zip_safe=False
)

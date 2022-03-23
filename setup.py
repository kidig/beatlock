from setuptools import setup, find_packages
import pathlib

from beatlock import __author__, __version__

here = pathlib.Path(__file__).parent.resolve()
requirements = (here / 'requirements.txt').read_text().splitlines() + ["setuptools"]
long_description = (here / 'README.md').read_text(encoding='utf-8')


setup(
    name='beatlock',
    author=__author__,
    version=__version__,
    url='https://github.com/kidig/beatlock',
    description='Celerybeat scheduler on redis-sentinel',
    long_description=long_description,
    long_description_content_type='text/markdown',
    license='MIT',
    packages=find_packages(),
    install_requires=requirements,
    keywords=' '.join([
        'celerybeat',
        'redis',
        'sentinel',
        'scheduler',
    ]),
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Development Status :: 4 - Beta',
    ],
)
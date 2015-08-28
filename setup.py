import io
import os
import re
from setuptools import setup, find_packages

from performanceplatform import collector

# multiprocessing and logging don't get on with each other in Python
# vesions < 2.7.4. The following unused import is a workaround. See:
# http://bugs.python.org/issue15881#msg170215
import multiprocessing


def _read(fname, fail_silently=False):
    """
    Read the content of the given file. The path is evaluated from the
    directory containing this file.
    """
    try:
        filepath = os.path.join(os.path.dirname(__file__), fname)
        with io.open(filepath, 'rt', encoding='utf8') as f:
            return f.read()
    except:
        if not fail_silently:
            raise
        return ''


def _get_version():
    data = _read(
        'performanceplatform/collector/__init__.py'
    )
    version = re.search(
        r"^__version__ = ['\"]([^'\"]*)['\"]",
        data,
        re.M | re.I
    ).group(1).strip()
    return version


def _get_requirements(fname):
    """
    Create a list of requirements from the output of the pip freeze command
    saved in a text file.
    """
    packages = _read(fname).split('\n')
    packages = (p.strip() for p in packages)
    packages = (p for p in packages if p and not p.startswith('#'))
    return list(packages)


def _get_long_description():
    return _read('README.rst')


if __name__ == '__main__':
    setup(
        name='performanceplatform-collector',
        version=_get_version(),
        packages=find_packages(exclude=['test*']),
        namespace_packages=['performanceplatform'],

        # metadata for upload to PyPI
        author=collector.__author__,
        author_email=collector.__author_email__,
        maintainer='Government Digital Service',
        url='https://github.com/alphagov/performanceplatform-collector',

        description='performanceplatform-collector: tools for sending '
            'data to the Performance Platform',
        long_description=_get_long_description(),
        license='MIT',
        keywords='api data performance_platform',

        install_requires=_get_requirements('requirements.txt'),
        tests_require=_get_requirements('requirements_for_tests.txt'),

        test_suite='nose.collector',

        entry_points={
            'console_scripts': [
                'pp-collector=performanceplatform.collector.main:main'
            ]
        }
    )
